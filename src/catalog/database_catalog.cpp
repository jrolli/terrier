#include <vector>

#include "catalog/catalog_defs.h"
#include "catalog/database_catalog.h"
#include "catalog/index_schema.h"
#include "catalog/postgres/pg_namespace.h"
#include "catalog/postgres/pg_type.h"
#include "catalog/postgres/pg_attribute.h"
#include "catalog/postgres/pg_class.h"
#include "catalog/postgres/pg_constraint.h"
#include "catalog/schema.h"
#include "storage/index/index.h"
#include "storage/sql_table.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

// namespace_oid_t DatabaseCatalog::CreateNamespace(transaction::TransactionContext *txn, const std::string &name);

// bool DatabaseCatalog::DeleteNamespace(transaction::TransactionContext *txn, namespace_oid_t ns);

// namespace_oid_t DatabaseCatalog::GetNamespaceOid(transaction::TransactionContext *txn, const std::string &name);

table_oid_t DatabaseCatalog::CreateTable(transaction::TransactionContext *txn, namespace_oid_t ns,
                                         const std::string &name, const Schema &schema) {
  const table_oid_t table_oid = static_cast<table_oid_t>(next_oid_++);
  return CreateTableEntry(txn, table_oid, ns, name, schema) ? table_oid : INVALID_TABLE_OID;
}

bool DatabaseCatalog::DeleteTable(transaction::TransactionContext *const txn, const table_oid_t table) {
  std::vector<storage::TupleSlot> index_results;

}

table_oid_t DatabaseCatalog::GetTableOid(transaction::TransactionContext *const txn, const namespace_oid_t ns,
                                         const std::string &name) {
  std::vector<storage::TupleSlot> index_results;
  auto name_pri = classes_name_index_->GetProjectedRowInitializer();

  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  byte *varlen_contents = nullptr;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    varlen_contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(varlen_contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(varlen_contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>(name.data()), name.size());
  }

  // Name is a larger projected row (16-byte key vs 4-byte key), sow we can reuse
  // the buffer for both index operations if we allocate to the larger one.
  auto *const buffer = common::AllocationUtil::AllocateAligned(name_pri.ProjectedRowSize());
  auto pr = name_pri.InitializeRow(buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(pr->AccessForceNotNull(0))) = name_varlen;

  classes_name_index_->ScanKey(*txn, *pr, &index_results);
  if (varlen_contents != nullptr) {
    delete[] varlen_contents;
  }

  if (index_results.empty()) {
    delete[] buffer;
    return INVALID_TABLE_OID;
  }
  TERRIER_ASSERT(index_results.size() == 1, "Table name not unique in index");

  const auto table_pri = classes_->InitializerForProjectedRow({RELOID_COL_OID, RELKIND_COL_OID}).first;
  TERRIER_ASSERT(table_pri.ProjectedRowSize() <= name_pri.ProjectedRowSize(),
                 "I want to reuse this buffer because I'm lazy and malloc is slow but it needs to be big enough.");
  pr = table_pri.InitializeRow(buffer);
  const auto result UNUSED_ATTRIBUTE = classes_->Select(txn, index_results[0], pr);
  TERRIER_ASSERT(result, "Index already verified visibility. This shouldn't fail.");

  // This code assumes ordering of attribute by size in the ProjectedRow (size of kind is smaller than size of oid)
  if (*(reinterpret_cast<const postgres::ClassKind *const>(pr->AccessForceNotNull(1))) != postgres::ClassKind::REGULAR_TABLE) {
    // User called GetTableOid on an object that doesn't have type REGULAR_TABLE
    return INVALID_TABLE_OID;
  }

  const auto table_oid = *(reinterpret_cast<const table_oid_t *const>(pr->AccessForceNotNull(0)));
  delete[] buffer;
  return table_oid;
}

// bool DatabaseCatalog::RenameTable(transaction::TransactionContext *txn, table_oid_t table, const std::string &name);

// bool DatabaseCatalog::UpdateSchema(transaction::TransactionContext *txn, table_oid_t table, Schema *new_schema);

// const Schema &DatabaseCatalog::GetSchema(transaction::TransactionContext *txn, table_oid_t table);

// std::vector<constraint_oid_t> DatabaseCatalog::GetConstraints(transaction::TransactionContext *txn, table_oid_t);

// std::vector<index_oid_t> DatabaseCatalog::GetIndexes(transaction::TransactionContext *txn, table_oid_t);

// index_oid_t DatabaseCatalog::CreateIndex(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name,
//                                          table_oid_t table, IndexSchema *schema);

// bool DatabaseCatalog::DeleteIndex(transaction::TransactionContext *txn, index_oid_t index);

// index_oid_t DatabaseCatalog::GetIndexOid(transaction::TransactionContext *txn, namespace_oid_t ns, const std::string &name);

// const IndexSchema &DatabaseCatalog::GetIndexSchema(transaction::TransactionContext *txn, index_oid_t index);

void DatabaseCatalog::TearDown(transaction::TransactionContext *txn) {
  std::vector<parser::AbstractExpression *> expressions;
  std::vector<Schema *> table_schemas;
  std::vector<storage::SqlTable *> tables;
  std::vector<IndexSchema *> index_schemas;
  std::vector<storage::index::Index *> indexes;

  std::vector<col_oid_t> col_oids;

  // pg_class (schemas & objects) [this is the largest projection]
  col_oids.emplace_back(RELKIND_COL_OID);
  col_oids.emplace_back(REL_SCHEMA_COL_OID);
  col_oids.emplace_back(REL_PTR_COL_OID);
  [pci, pm] = classes_->InitializerForProjectedColumns(col_oids, 100);

  byte *buffer = common::AllocationUtil::AllocateAligned(pci.ProjectedColumnsSize());
  auto pc = pci.Initialize(buffer);

  // Fetch pointers to the start each in the projected columns
  auto classes = reinterpret_cast<postgres::ClassKind *>pc->ColumnStart(pm[RELKIND_COL_OID]);
  auto schemas = reinterpret_cast<void **>pc->ColumnStart(pm[REL_SCHEMA_COL_OID]);
  auto objects = reinterpret_cast<void **>pc->ColumnStart(pm[REL_PTR_COL_OID]);

  // Scan the table
  auto table_iter = classes_->begin();
  while (table_iter != classes_->end()) {
    classes_->Scan(txn, table_iter, pc);
    for (int i = 0; i < pc->NumTuples()) {
      switch(classes[i]) {
        case postgres::ClassKind::REGULAR_TABLE:
          table_schemas.emplace_back(reinterpret_cast<Schema *>schemas[i]);
          tables.emplace_back(reinterpret_cast<storage::SqlTable *>objects[i]);
          break;
        case postgres::ClassKind::INDEX:
          index_schemas.emplace_back(reinterpret_cast<IndexSchema *>schemas[i]);
          indexes.emplace_back(reinterpret_cast<storage::index::Index *>objects[i]);
          break;
        default:
          throw std::runtime_error("Unimplemented destructor needed");
      }
    }
  }

  // pg_attribute (expressions)
  col_oids.clear();
  col_oids.emplace_back(ADBIN_COL_OID);
  [pci, pm] = columns_->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>pc->ColumnStart(0);

  table_iter = columns_->begin();
  while (table_iter != columns_->end()) {
    columns_->Scan(txn, table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // pg_constraint (expressions)
  col_oids.clear();
  col_oids.emplace_back(CONBIN_COL_OID);
  [pci, pm] = constraints->InitializerForProjectedColumns(col_oids, 100);
  pc = pci.Initialize(buffer);

  auto exprs = reinterpret_cast<parser::AbstractExpression **>pc->ColumnStart(0);

  table_iter = constraints->begin();
  while (table_iter != constraints->end()) {
    constraints->Scan(txn, table_iter, pc);

    for (int i = 0; i < pc->NumTuples()) {
      expressions.emplace_back(exprs[i]);
    }
  }

  // No new transactions can see these object but there may be deferred index
  // and other operation.  Therefore, we need to defer the deallocation on delete
  txn->RegisterCommitAction([=, tables{std::move(tables)}, indexes{std::move(indexes)},
                                table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schema)},
                                expressions{std::move(expressions)}] {
    txn->GetTransactionManager()->DeferAction([=, tables{std::move(tables)}, indexes{std::move(indexes)},
                                                  table_schemas{std::move(table_schemas)}, index_schemas{std::move(index_schema)},
                                                  expressions{std::move(expressions)}] {
      for (auto table : tables)
        delete table;

      for (auto index : indexes)
        delete index;

      for (auto schema : table_schemas)
        delete schema;

      for (auto schema : index_schemas)
        delete schema;

      for (auto expr : expressions)
        delete expr;
    });
  });
}

type_oid_t DatabaseCatalog::GetTypeOidForType(type::TypeId type) { return type_oid_t(static_cast<uint8_t>(type)); }

void DatabaseCatalog::InsertType(transaction::TransactionContext *txn, type::TypeId internal_type,
                                 const std::string &name, namespace_oid_t namespace_oid, int16_t len, bool by_val,
                                 postgres::Type type_category) {
  std::vector<col_oid_t> table_col_oids;
  table_col_oids.emplace_back(TYPOID_COL_OID);
  table_col_oids.emplace_back(TYPNAME_COL_OID);
  table_col_oids.emplace_back(TYPNAMESPACE_COL_OID);
  table_col_oids.emplace_back(TYPLEN_COL_OID);
  table_col_oids.emplace_back(TYPBYVAL_COL_OID);
  table_col_oids.emplace_back(TYPTYPE_COL_OID);
  auto initializer_pair = types_->InitializerForProjectedRow(table_col_oids);
  auto initializer = initializer_pair.first;
  auto col_map = initializer_pair.second;

  // Stage the write into the table
  auto redo_record = txn->StageWrite(db_oid_, TYPE_TABLE_OID, initializer);
  auto *delta = redo_record->Delta();

  // Populate oid
  auto offset = col_map[TYPOID_COL_OID];
  auto type_oid = GetTypeOidForType(internal_type);
  memcpy(delta->AccessForceNotNull(offset), &type_oid, sizeof(type_oid_t));

  // Populate type name
  offset = col_map[TYPNAME_COL_OID];
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<byte *>(name.data()), name.size());
  }
  *(reinterpret_cast<storage::VarlenEntry *>(delta->AccessForceNotNull(offset))) = name_varlen;

  // Populate namespace
  offset = col_map[TYPNAMESPACE_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &namespace_oid, sizeof(namespace_oid_t));

  // Populate len
  offset = col_map[TYPLEN_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &len, sizeof(int16_t) /* SMALLINT */);

  // Populate byval
  offset = col_map[TYPBYVAL_COL_OID];
  memcpy(delta->AccessForceNotNull(offset), &by_val, sizeof(bool));

  // Populate type
  offset = col_map[TYPTYPE_COL_OID];
  uint8_t type = static_cast<uint8_t>(type_category);
  memcpy(delta->AccessForceNotNull(offset), &type, sizeof(uint8_t) /* TINYINT */);

  // Insert into table
  auto tuple_slot = types_->Insert(txn, redo_record);

  // Allocate buffer of largest size needed
  uint32_t buffer_size = 0;
  buffer_size = std::max(buffer_size, types_oid_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  buffer_size = std::max(buffer_size, types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  byte *buffer = common::AllocationUtil::AllocateAligned(buffer_size);

  // Insert into oid index
  auto oid_index_delta = types_oid_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto oid_index_offset = types_oid_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(oid_index_delta->AccessForceNotNull(oid_index_offset), &type_oid, sizeof(type_oid_t));
  auto result UNUSED_ATTRIBUTE = types_oid_index_->InsertUnique(txn, *oid_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type oid index should always succeed");

  // Insert into (namespace_oid, name) index
  buffer = common::AllocationUtil::AllocateAligned(types_name_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto name_index_delta = types_name_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  // Populate namespace
  auto name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(name_index_delta->AccessForceNotNull(name_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  // Populate type name
  name_index_offset = types_name_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(2));
  *(reinterpret_cast<storage::VarlenEntry *>(name_index_delta->AccessForceNotNull(name_index_offset))) = name_varlen;
  result = types_name_index_->InsertUnique(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type name index should always succeed");

  // Insert into (non-unique) namespace oid index
  buffer =
      common::AllocationUtil::AllocateAligned(types_namespace_index_->GetProjectedRowInitializer().ProjectedRowSize());
  auto namespace_index_delta = types_namespace_index_->GetProjectedRowInitializer().InitializeRow(buffer);
  auto namespace_index_offset = types_namespace_index_->GetKeyOidToOffsetMap().at(catalog::indexkeycol_oid_t(1));
  memcpy(namespace_index_delta->AccessForceNotNull(namespace_index_offset), &namespace_oid, sizeof(namespace_oid_t));
  result = types_namespace_index_->Insert(txn, *name_index_delta, tuple_slot);
  TERRIER_ASSERT(result, "Insert into type namespace index should always succeed");

  // Clean up buffer
  delete[] buffer;
}

void DatabaseCatalog::BootstrapTypes(transaction::TransactionContext *txn) {
  InsertType(txn, type::TypeId::INVALID, "INVALID", NAMESPACE_CATALOG_NAMESPACE_OID, 1, true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::BOOLEAN, "BOOLEAN", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(bool), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TINYINT, "TINYINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int8_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::SMALLINT, "SMALLINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int16_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::INTEGER, "INTEGER", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int32_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::BIGINT, "BIGINT", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(int64_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::DECIMAL, "DECIMAL", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(double), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::TIMESTAMP, "TIMESTAMP", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::timestamp_t),
             true, postgres::Type::BASE);

  InsertType(txn, type::TypeId::DATE, "DATE", NAMESPACE_CATALOG_NAMESPACE_OID, sizeof(type::date_t), true,
             postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARCHAR, "VARCHAR", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false, postgres::Type::BASE);

  InsertType(txn, type::TypeId::VARBINARY, "VARBINARY", NAMESPACE_CATALOG_NAMESPACE_OID, -1, false,
             postgres::Type::BASE);
}

bool DatabaseCatalog::CreateTableEntry(transaction::TransactionContext *const txn, const table_oid_t table_oid,
                                       const namespace_oid_t ns_oid, const std::string &name, const Schema &schema) {
  auto [pr_init, pr_map] = classes_->InitializerForProjectedRow(PG_CLASS_ALL_COL_OIDS);

  auto *const insert_redo = txn->StageWrite(db_oid_, table_oid, pr_init);
  auto *const insert_pr = insert_redo->Delta();

  // Write the ns_oid into the PR
  const auto ns_offset = pr_map[RELNAMESPACE_COL_OID];
  auto *const ns_ptr = insert_pr->AccessForceNotNull(ns_offset);
  *(reinterpret_cast<uint32_t *>(ns_ptr)) = static_cast<uint32_t>(ns_oid);

  // Write the table_oid into the PR
  const auto table_oid_offset = pr_map[RELOID_COL_OID];
  auto *const table_oid_ptr = insert_pr->AccessForceNotNull(table_oid_offset);
  *(reinterpret_cast<uint32_t *>(table_oid_ptr)) = static_cast<uint32_t>(table_oid);

  // Write the col oids into a new Schema object
  col_oid_t next_col_oid(1);
  auto *const schema_ptr = new Schema(schema);
  // TODO(Matt): when AbstractExpressions are added to Schema::Column as a field for default
  // value, we need to make sure Column gets a properly written copy constructor to deep
  // copy those to guarantee that this copy mechanism still works
  for (auto &column : schema_ptr->columns_) {
    column.oid_ = next_col_oid++;
  }

  // Write the next_col_oid into the PR
  const auto next_col_oid_offset = pr_map[REL_NEXTCOLOID_COL_OID];
  auto *const next_col_oid_ptr = insert_pr->AccessForceNotNull(next_col_oid_offset);
  *(reinterpret_cast<uint32_t *>(next_col_oid_ptr)) = static_cast<uint32_t>(next_col_oid);

  // Write the schema_ptr into the PR
  const auto schema_ptr_offset = pr_map[REL_SCHEMA_COL_OID];
  auto *const schema_ptr_ptr = insert_pr->AccessForceNotNull(schema_ptr_offset);
  *(reinterpret_cast<uintptr_t *>(schema_ptr_ptr)) = reinterpret_cast<uintptr_t>(schema_ptr_ptr);

  // Set table_ptr to NULL because it gets set by execution layer after instantiation
  const auto table_ptr_offset = pr_map[REL_PTR_COL_OID];
  insert_pr->SetNull(table_ptr_offset);

  // Write the kind into the PR
  const auto kind_offset = pr_map[RELKIND_COL_OID];
  auto *const kind_ptr = insert_pr->AccessForceNotNull(kind_offset);
  *(reinterpret_cast<char *>(kind_ptr)) = static_cast<char>(postgres::ClassKind::REGULAR_TABLE);

  // Create the necessary varlen for storage operations
  storage::VarlenEntry name_varlen;
  if (name.size() > storage::VarlenEntry::InlineThreshold()) {
    byte *contents = common::AllocationUtil::AllocateAligned(name.size());
    std::memcpy(contents, name.data(), name.size());
    name_varlen = storage::VarlenEntry::Create(contents, name.size(), true);
  } else {
    name_varlen = storage::VarlenEntry::CreateInline(reinterpret_cast<const byte *const>(name.data()), name.size());
  }

  // Write the name into the PR
  const auto name_offset = pr_map[RELNAME_COL_OID];
  auto *const name_ptr = insert_pr->AccessForceNotNull(name_offset);
  *(reinterpret_cast<storage::VarlenEntry *>(name_ptr)) = name_varlen;

  // Insert into pg_class table
  const auto tuple_slot = classes_->Insert(txn, insert_redo);

  // Get PR initializers and allocate a buffer from the largest one
  const auto oid_index_init = classes_oid_index_->GetProjectedRowInitializer();
  const auto name_index_init = classes_name_index_->GetProjectedRowInitializer();
  const auto ns_index_init = classes_namespace_index_->GetProjectedRowInitializer();
  auto *const index_buffer = common::AllocationUtil::AllocateAligned(name_index_init.ProjectedRowSize());

  // Insert into oid_index
  auto *index_pr = oid_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<uint32_t *>(index_pr->AccessForceNotNull(0))) = static_cast<uint32_t>(table_oid);
  if (!classes_oid_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was an oid conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into name_index
  index_pr = name_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<storage::VarlenEntry *>(index_pr->AccessForceNotNull(0))) = name_varlen;
  if (!classes_name_index_->InsertUnique(txn, *index_pr, tuple_slot)) {
    // There was a name conflict and we need to abort.  Free the buffer and
    // return INVALID_TABLE_OID to indicate the database was not created.
    delete[] index_buffer;
    return false;
  }

  // Insert into namespace_index
  index_pr = ns_index_init.InitializeRow(index_buffer);
  *(reinterpret_cast<uint32_t *>(index_pr->AccessForceNotNull(0))) = static_cast<uint32_t>(ns_oid);
  const auto result UNUSED_ATTRIBUTE = classes_namespace_index_->Insert(txn, *index_pr, tuple_slot);
  TERRIER_ASSERT(!result, "Insertion into non-unique namespace index failed.");

  delete[] index_buffer;

  return true;
}

}  // namespace terrier::catalog
