#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/exec/execution_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/sql/sql.h"
#include "execution/sql/vector.h"
#include "execution/table_generator/table_generator.h"
#include "execution/tpl_test.h"
#include "gtest/gtest.h"
#include "main/db_main.h"
#include "optimizer/statistics/stats_storage.h"
#include "storage/garbage_collector.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/timestamp_manager.h"

namespace noisepage::execution {

class SqlBasedTest : public TplTest {
 public:
  SqlBasedTest() = default;

  void SetUp() override {
    TplTest::SetUp();
    // Initialize noisepage objects

    db_main_ = noisepage::DBMain::Builder()
                   .SetUseGC(true)
                   .SetUseGCThread(true)
                   .SetUseCatalog(true)
                   .SetUseStatsStorage(true)
                   .Build();
    metrics_manager_ = db_main_->GetMetricsManager();

    block_store_ = db_main_->GetStorageLayer()->GetBlockStore();
    catalog_ = db_main_->GetCatalogLayer()->GetCatalog();
    txn_manager_ = db_main_->GetTransactionLayer()->GetTransactionManager();
    stats_storage_ = db_main_->GetStatsStorage();

    test_txn_ = txn_manager_->BeginTransaction();

    // Create catalog and test namespace
    test_db_oid_ = catalog_->CreateDatabase(common::ManagedPointer(test_txn_), "test_db", true);
    ASSERT_NE(test_db_oid_, catalog::INVALID_DATABASE_OID) << "Default database does not exist";
    accessor_ = catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_, DISABLED);
    test_ns_oid_ = accessor_->GetDefaultNamespace();

    exec_settings_ = std::make_unique<exec::ExecutionSettings>();
  }

  ~SqlBasedTest() override { txn_manager_->Commit(test_txn_, transaction::TransactionUtil::EmptyCallback, nullptr); }

  catalog::namespace_oid_t NSOid() { return test_ns_oid_; }

  common::ManagedPointer<storage::BlockStore> BlockStore() { return block_store_; }

  std::unique_ptr<exec::ExecutionContext> MakeExecCtx(exec::OutputCallback *callback = nullptr,
                                                      const planner::OutputSchema *schema = nullptr) {
    exec::OutputCallback empty = nullptr;
    const auto &callback_ref = (callback == nullptr) ? empty : *callback;
    return std::make_unique<exec::ExecutionContext>(test_db_oid_, common::ManagedPointer(test_txn_), callback_ref,
                                                    schema, common::ManagedPointer(accessor_), *exec_settings_,
                                                    metrics_manager_, DISABLED, DISABLED);
  }

  void GenerateTestTables(exec::ExecutionContext *exec_ctx) {
    sql::TableGenerator table_generator{exec_ctx, block_store_, test_ns_oid_};
    table_generator.GenerateTestTables();
  }

  std::unique_ptr<noisepage::catalog::CatalogAccessor> MakeAccessor() {
    return catalog_->GetAccessor(common::ManagedPointer(test_txn_), test_db_oid_, DISABLED);
  }

 protected:
  std::unique_ptr<catalog::CatalogAccessor> accessor_;
  catalog::db_oid_t test_db_oid_{0};
  common::ManagedPointer<optimizer::StatsStorage> stats_storage_;
  transaction::TransactionContext *test_txn_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;

 private:
  std::unique_ptr<DBMain> db_main_;
  common::ManagedPointer<metrics::MetricsManager> metrics_manager_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  catalog::namespace_oid_t test_ns_oid_;
  std::unique_ptr<exec::ExecutionSettings> exec_settings_;
};

static inline std::unique_ptr<sql::Vector> MakeVector(sql::TypeId type_id, uint32_t size) {  // NOLINT
  auto vec = std::make_unique<sql::Vector>(type_id, true, true);
  vec->Resize(size);
  return vec;
}

#define MAKE_VEC_TYPE(TYPE, CPP_TYPE)                                                              \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(uint32_t size) { /* NOLINT */      \
    return MakeVector(sql::TypeId::TYPE, size);                                                    \
  }                                                                                                \
  static inline std::unique_ptr<sql::Vector> Make##TYPE##Vector(/* NOLINT */                       \
                                                                const std::vector<CPP_TYPE> &vals, \
                                                                const std::vector<bool> &nulls) {  \
    NOISEPAGE_ASSERT(vals.size() == nulls.size(), "Value and NULL vector sizes don't match");      \
    auto vec = Make##TYPE##Vector(vals.size());                                                    \
    for (uint64_t i = 0; i < vals.size(); i++) {                                                   \
      if (nulls[i]) {                                                                              \
        vec->SetValue(i, sql::GenericValue::CreateNull(vec->GetTypeId()));                         \
      } else {                                                                                     \
        vec->SetValue(i, sql::GenericValue::Create##TYPE(vals[i]));                                \
      }                                                                                            \
    }                                                                                              \
    return vec;                                                                                    \
  }

MAKE_VEC_TYPE(Boolean, bool)
MAKE_VEC_TYPE(TinyInt, int8_t)
MAKE_VEC_TYPE(SmallInt, int16_t)
MAKE_VEC_TYPE(Integer, int32_t)
MAKE_VEC_TYPE(BigInt, int64_t)
MAKE_VEC_TYPE(Float, float)
MAKE_VEC_TYPE(Double, double)
MAKE_VEC_TYPE(Date, sql::Date)
MAKE_VEC_TYPE(Varchar, std::string_view)
MAKE_VEC_TYPE(Pointer, uintptr_t);

#undef MAKE_VEC_TYPE

}  // namespace noisepage::execution
