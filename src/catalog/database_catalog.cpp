#pragma once

#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/index_schema.h"
#include "catalog/schema.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"

#include "catalog/database_catalog.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_attribute.h"


namespace terrier::catalog {

namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name) {
  namespace_oid_t ns_oid{next_oid_++};
  if (CreateNamespace(txn, name, ns_oid)) {
    return INVALID_NAMESPACE_OID;
  }
  return ns_oid;
}

bool DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name, namespace_oid_t ns_oid) {
  // Step 1: Insert into table
  storage::VarlenEntry name_varlen = postgres::AttributeHelper::CreateVarlen(name);
  // Get & Fill Redo Record
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID, NSPOID_COL_OID};
  // NOLINTNEXTLINE (C++17 only)
  auto [pri, pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto *redo = txn->StageWrite(db_oid_, NAMESPACE_TABLE_OID, pri);
  auto *oid_entry = reinterpret_cast<namespace_oid_t *>(redo->Delta()->AccessForceNotNull(pm[NSPOID_COL_OID]));
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(pm[NSPNAME_COL_OID]));
  *oid_entry = ns_oid;
  *name_entry = name_varlen;
  // Finally, insert into the table to get the tuple slot
  auto tupleslot = namespaces_->Insert(txn, redo);

  // Step 2: Insert into name index
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  byte *buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto  *pr = name_pri.InitializeRow(buffer);
  name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = name_varlen;

  if (!namespaces_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_DATABASE_OID to indicate the database was not created.
    delete[] buffer;
    return false;
  }

  // Step 3: Insert into oid index
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  // Reuse buffer since an u32 column is smaller than a varlen column
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));
  *oid_entry = ns_oid;
  const bool UNUSED_ATTRIBUTE result = namespaces_oid_index_->InsertUnique(txn, *pr, tupleslot);
  TERRIER_ASSERT(result, "Assigned namespace OID failed to be unique");

  delete[] buffer;
  return true;
}


bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns) {
  // Step 1: Read the oid index
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID};
  // NOLINTNEXTLINE (C++17 only)
  auto [table_pri, table_pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  auto oid_pri = namespaces_oid_index_->GetProjectedRowInitializer();
  // Buffer is large enough for all prs.
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  auto pr = oid_pri.InitializeRow(buffer);
  // Scan index
  auto *oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(0));
  *oid_entry = ns;
  std::vector<storage::TupleSlot> index_results;
  namespaces_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty()) {
    delete[] buffer;
    return false;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Namespace OID not unique in index");

  // Step 2: Select from the table to get the name
  pr = table_pri.InitializeRow(buffer);
  if (!namespaces_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return false;
  }
  auto name_varlen = *reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(table_pm[NSPNAME_COL_OID]));


  // Step 3: Delete from table
  if (!namespaces_->Delete(txn, index_results[0]))
  {
    // Someone else has a write-lock
    delete[] buffer;
    return false;
  }

  // Step 4: Delete from oid index
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(table_pm[NSPOID_COL_OID]));
  *oid_entry = ns;
  namespaces_oid_index_->Delete(txn, *pr, index_results[0]);

  // Step 5: Delete from name index
  pr = name_pri.InitializeRow(buffer);
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(table_pm[NSPNAME_COL_OID]));
  *name_entry = name_varlen;
  namespaces_name_index_->Delete(txn, *pr, index_results[0]);

  // Finish
  delete[] buffer;
  return true;
}

namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name) {
  // Step 1: Read the name index
  std::vector<col_oid_t> table_oids{NSPNAME_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = namespaces_->InitializerForProjectedRow(table_oids);
  auto name_pri = namespaces_name_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  // Scan the name index
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = postgres::AttributeHelper::CreateVarlen(name);
  std::vector<storage::TupleSlot> index_results;
  namespaces_name_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty())
  {
    delete[] buffer;
    return INVALID_NAMESPACE_OID;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Namespace name not unique in index");

  // Step 2: Scan the table to get the oid
  pr = table_pri.InitializeRow(buffer);
  if (!namespaces_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return INVALID_NAMESPACE_OID;
  }
  auto ns_oid = *reinterpret_cast<namespace_oid_t *>(pr->AccessForceNotNull(table_pm[NSPOID_COL_OID]));
  delete[] buffer;
  return ns_oid;
}

template <typename Column>
bool DatabaseCatalog::CreateTableAttribute(transaction::TransactionContext *txn, uint32_t class_oid, const Column &col, const parser::AbstractExpression *  default_val) {
  // Step 1: Insert into the table
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTRELID_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID, ADSRC_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto *redo = txn->StageWrite(db_oid_, COLUMN_TABLE_OID, table_pri);
  auto oid_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNUM_COL_OID]));
  auto relid_entry = reinterpret_cast<uint32_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTRELID_COL_OID]));
  auto name_entry = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNAME_COL_OID]));
  auto type_entry = reinterpret_cast<type::TypeId *>(redo->Delta()->AccessForceNotNull(table_pm[ATTTYPID_COL_OID]));
  auto len_entry = reinterpret_cast<uint16_t *>(redo->Delta()->AccessForceNotNull(table_pm[ATTLEN_COL_OID]));
  auto notnull_entry = reinterpret_cast<bool *>(redo->Delta()->AccessForceNotNull(table_pm[ATTNOTNULL_COL_OID]));
  auto dbin_entry = reinterpret_cast<intptr_t *>(redo->Delta()->AccessForceNotNull(table_pm[ADBIN_COL_OID]));
  auto dsrc_entry = reinterpret_cast<storage::VarlenEntry *>(redo->Delta()->AccessForceNotNull(table_pm[ADSRC_COL_OID]));
  *oid_entry = !col.GetOid();
  *relid_entry = class_oid;
  storage::VarlenEntry name_varlen = postgres::AttributeHelper::MakeNameVarlen<Column>(col);
  *name_entry = name_varlen;
  *type_entry = col.GetType();
  // TODO(Amadou): Figure out what really goes here for varlen
  *len_entry = (col.GetType() == type::TypeId::VARCHAR || col.GetType() == type::TypeId::VARBINARY) ? col.GetMaxVarlenSize() : col.GetAttrSize();
  *notnull_entry = !col.GetNullable();
  *dbin_entry = reinterpret_cast<intptr_t>(default_val);
  storage::VarlenEntry dsrc_varlen = postgres::AttributeHelper::CreateVarlen(default_val->ToJson().dump());
  *dsrc_entry = dsrc_varlen;
  // Finally insert
  auto tupleslot = columns_->Insert(txn, redo);

  // Step 2: Insert into name index
  auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Create a buffer large enough for all columns
  auto buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = name_varlen;
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;


  if (!columns_name_index_->InsertUnique(txn, *pr, tupleslot)) {
    delete[] buffer;
    return false;
  }

  // Step 3: Insert into oid index
  auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  pr = oid_pri.InitializeRow(buffer);
  oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *oid_entry = col.GetOid();
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;


  if (!columns_oid_index_->InsertUnique(txn, *pr, tupleslot)) {
    delete[] buffer;
    return false;
  }

  // Step 4: Insert into class index
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  pr = class_pri.InitializeRow(buffer);
  relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *relid_entry = class_oid;


  if (!columns_class_index_->Insert(txn, *pr, tupleslot)) {
    // Should probably not happen.
    delete[] buffer;
    return false;
  }

  delete[] buffer;
  return true;
}


template <typename Column>
std::unique_ptr<Column> DatabaseCatalog::GetTableAttribute(transaction::TransactionContext *txn, storage::VarlenEntry * col_name, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto name_pri = columns_name_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the name index
  auto pr = name_pri.InitializeRow(buffer);
  auto *name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
  *name_entry = *col_name;
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_name_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty())
  {
    delete[] buffer;
    return nullptr;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Columns name not unique in index");

  // Step 2: Scan the table to get the column
  pr = table_pri.InitializeRow(buffer);
  if (!columns_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return nullptr;
  }
  auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
  delete[] buffer;
  return col;
}

template <typename Column>
std::unique_ptr<Column> DatabaseCatalog::GetTableAttribute(transaction::TransactionContext *txn, uint32_t col_oid, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the oid index
  auto pr = oid_pri.InitializeRow(buffer);
  auto *oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
  *oid_entry = col_oid;
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_oid_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty())
  {
    delete[] buffer;
    return nullptr;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Columns oid not unique in index");

  // Step 2: Scan the table to get the column
  pr = table_pri.InitializeRow(buffer);
  if (!columns_->Select(txn, index_results[0], pr)) {
    // Nothing visible
    delete[] buffer;
    return nullptr;
  }
  auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
  delete[] buffer;
  return col;
}

template <typename Column>
std::vector<std::unique_ptr<Column>> DatabaseCatalog::GetTableAttributes(transaction::TransactionContext *txn, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the class index
  auto pr = class_pri.InitializeRow(buffer);
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_class_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty())
  {
    delete[] buffer;
    return nullptr;
  }

  // Step 2: Scan the table to get the columns
  std::vector<std::unique_ptr<Column>> cols;
  pr = table_pri.InitializeRow(buffer);
  for (const auto &slot: index_results) {
    if (!columns_->Select(txn, slot, pr)) {
      // Nothing visible
      delete[] buffer;
      return nullptr;
    }
    cols.emplace_back(postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm));
  }
  delete[] buffer;
  return cols;
}

template <typename Column>
void DatabaseCatalog::DeleteColumns(transaction::TransactionContext *txn, uint32_t class_oid) {
  // Step 1: Read Index
  std::vector<col_oid_t> table_oids{ATTNUM_COL_OID, ATTNAME_COL_OID, ATTTYPID_COL_OID, ATTLEN_COL_OID, ATTNOTNULL_COL_OID, ADBIN_COL_OID};
  // NOLINTNEXTLINE
  auto [table_pri, table_pm] = columns_->InitializerForProjectedRow(table_oids);
  auto class_pri = columns_class_index_->GetProjectedRowInitializer();
  // Buffer is large enough to hold all prs
  byte *buffer = common::AllocationUtil::AllocateAligned(table_pri.ProjectedRowSize());
  // Scan the class index
  auto pr = class_pri.InitializeRow(buffer);
  auto *relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
  *relid_entry = class_oid;
  std::vector<storage::TupleSlot> index_results;
  columns_class_index_->ScanKey(*txn, *pr, &index_results);
  if (index_results.empty())
  {
    delete[] buffer;
  }

  // Step 2: Scan the table to get the columns
  pr = table_pri.InitializeRow(buffer);
  for (const auto &slot: index_results) {
    if (!columns_->Select(txn, slot, pr)) {
      // Nothing visible
      delete[] buffer;
      return;
    }
    auto col = postgres::AttributeHelper::MakeColumn<Column>(pr, table_pm);
    // 1. Delete from class index
    class_pri = columns_class_index_->GetProjectedRowInitializer();
    pr = class_pri.InitializeRow(buffer);
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
    *relid_entry = class_oid;
    columns_class_index_->Delete(txn, *pr, slot);

    // 2. Delete from oid index
    auto oid_pri = columns_oid_index_->GetProjectedRowInitializer();
    pr = oid_pri.InitializeRow(buffer);
    auto oid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(0));
    *oid_entry = !col->GetOid();
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
    *relid_entry = class_oid;
    columns_oid_index_->Delete(txn, *pr, slot);

    // 3. Delete from name index
    auto name_pri = columns_name_index_->GetProjectedRowInitializer();
    pr = name_pri.InitializeRow(buffer);
    auto name_entry = reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0));
    *name_entry = postgres::AttributeHelper::MakeNameVarlen(*col);
    relid_entry = reinterpret_cast<uint32_t *>(pr->AccessForceNotNull(1));
    *relid_entry = class_oid;
    columns_name_index_->Delete(txn, *pr, slot);
  }
  delete[] buffer;
}


table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          const Schema &schema);

bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *txn, table_oid_t table);

table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
                          table_oid_t table, IndexSchema *schema);

bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

} // namespace terrier::catalog
