#pragma once

#include <map>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/legacy/json.h"
#include "future_lite/Executor.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/tester/NaviResourceHelper.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h" // put into first line for ha3 plugins ut access control

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

namespace navi {
class GraphMemoryPoolR;
class KernelTester;
class KernelTesterBuilder;
class NaviLoggerProvider;
} // namespace navi

namespace build_service::config {
class ResourceReader;
}

namespace sql {

class SqlQueryResource;
class SqlQueryConfigData;

class OpTestBase : public TESTBASE, public table::TableTestUtil {
public:
    OpTestBase();
    virtual ~OpTestBase();
    void SetUp() override;
    void TearDown() override;
    int64_t getRunId();
    navi::GraphMemoryPoolR *getMemPoolResource();
    void prepareIndex();
    virtual void prepareTablet();
    virtual void prepareUserIndex();
    void prepareKkvTable();
    virtual void prepareKkvTableData(std::string &tableName,
                                     std::string &fields,
                                     std::string &pkeyField,
                                     std::string &skeyField,
                                     std::string &attributes,
                                     std::string &docs,
                                     int64_t &ttl);
    indexlib::partition::IndexPartitionPtr
    makeIndexPartitionForKkvTable(const std::string &rootPath,
                                  const std::string &tableName,
                                  const std::string &fieldInfos,
                                  const std::string &pkeyName,
                                  const std::string &skeyName,
                                  const std::string &valueInfos,
                                  const std::string &docStrs,
                                  int64_t ttl);
    void prepareKvTable();
    virtual void prepareKvTableData(std::string &tableName,
                                    std::string &fields,
                                    std::string &pkeyField,
                                    std::string &attributes,
                                    std::string &docs,
                                    bool &enableTTL,
                                    int64_t &ttl);

    indexlib::partition::IndexPartitionPtr
    makeIndexPartitionForKvTable(const std::string &rootPath,
                                 const std::string &tableName,
                                 const std::string &fieldInfos,
                                 const std::string &pkeyName,
                                 const std::string &valueInfos,
                                 const std::string &docStrs,
                                 bool &enableTTL,
                                 int64_t ttl);

    virtual void prepareInvertedTable();
    virtual void prepareInvertedTableData(std::string &tableName,
                                          std::string &fields,
                                          std::string &indexes,
                                          std::string &attributes,
                                          std::string &summarys,
                                          std::string &truncateProfileStr,
                                          std::string &docs,
                                          int64_t &ttl);

    indexlib::partition::IndexPartitionPtr
    makeIndexPartitionForEmbeddingTable(const std::string &indexPath,
                                        const std::string &tableName,
                                        const std::string &schemaPath,
                                        const std::string &schemaFileName,
                                        const std::string &dataPath,
                                        const std::string &pluginPath,
                                        int64_t ttl);

    indexlib::partition::IndexPartitionPtr
    makeIndexPartitionForIndexTable(const std::string &rootPath,
                                    const std::string &tableName,
                                    const std::string &fieldInfos,
                                    const std::string &indexInfos,
                                    const std::string &attrInfos,
                                    const std::string &summaryInfos,
                                    const std::string &docStrs);

    void createSqlQueryResourceForTest(std::shared_ptr<SqlQueryResource> &sqlQueryResource);
    indexlib::partition::IndexPartitionPtr
    makeIndexPartitionForInvertedTable(const std::string &rootPath,
                                       const std::string &tableName,
                                       const std::string &fields,
                                       const std::string &indexes,
                                       const std::string &attributes,
                                       const std::string &summarys,
                                       const std::string &truncateProfileStr,
                                       const std::string &docs,
                                       int64_t ttl);
    void setResource(navi::KernelTesterBuilder &testerBuilder);
    template <typename T>
    void compareSingleColumn(table::Table *table,
                             const std::string &colName,
                             const std::vector<T> &vals) {
        auto col = table->getColumn(colName);
        ASSERT_TRUE(col != nullptr);
        ASSERT_EQ(vals.size(), col->getRowCount());
        auto data = col->getColumnData<T>();
        ASSERT_TRUE(data != nullptr);
        for (size_t i = 0; i < vals.size(); i++) {
            EXPECT_TRUE(vals[i] == data->get(i))
                << colName << ": " << i << ": " << data->toString(i);
        }
    }

    void compareStringColumn(table::Table *table,
                             const std::string &colName,
                             const std::vector<std::string> &vals);

    template <typename T1, typename T2 = T1>
    void compareMultiColumn(table::Table *table,
                            const std::string &colName,
                            const std::vector<std::vector<T2>> &vals) {
        auto col = table->getColumn(colName);
        ASSERT_TRUE(col != nullptr);
        ASSERT_EQ(vals.size(), col->getRowCount());
        typedef autil::MultiValueType<T1> MultiValueType;
        auto data = col->getColumnData<MultiValueType>();
        ASSERT_TRUE(data != nullptr);
        for (size_t i = 0; i < vals.size(); i++) {
            const MultiValueType &actual = data->get(i);
            ASSERT_EQ(actual.size(), vals[i].size()) << colName << ":" << i;
            for (size_t j = 0; j < vals[i].size(); ++j) {
                EXPECT_TRUE(actual[j] == vals[i][j]) << colName << i << "," << j;
            }
        }
    }

    table::TablePtr getTable(navi::DataPtr data);

    template <typename T>
    table::ColumnData<T> getColumnData(const table::TablePtr &table, const std::string &colName) {
        auto column = table->getColumn(colName);
        ASSERT_TRUE(column != NULL);
        table::ColumnData<T> columnData = column->getColumnData<T>();
        return columnData;
    }

    navi::DataPtr createTable(matchdoc::MatchDocAllocatorPtr allocator,
                              std::vector<matchdoc::MatchDoc> docs);

    static void checkTraceCount(size_t expectedNums,
                                const std::string &filter,
                                const std::vector<std::string> &traces);

    static void checkTraceCount(size_t expectedNums,
                                const std::string &filter,
                                navi::NaviLoggerProvider &provider);

    template <typename T>
    void checkVector(const std::vector<T> &expects, const std::vector<T> &actuals) {
        ASSERT_EQ(expects.size(), actuals.size());
        for (size_t i = 0; i < expects.size(); i++) {
            ASSERT_EQ(expects[i], actuals[i]);
        }
    }

    void makeConfigCtx(const std::string &jsonStr, navi::KernelConfigContextPtr &ctx);

    void asyncRunKernel(navi::KernelTester &tester,
                        table::TablePtr &outputTable,
                        bool &eof,
                        int64_t timeout = 5 * 1000 * 1000);
    void asyncRunKernelToEof(navi::KernelTester &tester,
                             table::TablePtr &outputTable,
                             int64_t timeout = 5 * 1000 * 1000);
    navi::NaviResourceHelper *getNaviRHelper() {
        return &_naviRHelper;
    }

private:
    void prepareNamedData();
    void prepareResources();
    void prepareTabletManagerR();

public:
    std::shared_ptr<SqlQueryConfigData> _queryConfigData;
    navi::NaviResourceHelper _naviRHelper;
    std::vector<std::shared_ptr<void>> _dependentHolders;
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::MatchDocUtil _matchDocUtil;
    std::string _testPath;
    std::string _tableName;
    indexlib::partition::IndexApplication::IndexPartitionMap _indexPartitionMap;
    indexlib::partition::IndexApplicationPtr _indexApp;
    indexlib::partition::JoinRelationMap _joinMap;
    std::map<int32_t, indexlib::partition::IndexApplicationPtr> _id2IndexAppMap;

    indexlibv2::framework::TabletPtrMap _tabletMap;

    bool _needBuildIndex = false;
    bool _needBuildTablet = false;
    bool _needKvTable = false;
    bool _needKkvTable = false;
    bool _needExprResource = false;
    bool _needTracer = true;
    bool _needTerminator = true;
    std::vector<std::pair<navi::DataPtr, bool>> _inputs;
    std::vector<std::pair<navi::DataPtr, bool>> _outputs;
    autil::legacy::json::JsonMap _attributeMap;
    std::shared_ptr<build_service::config::ResourceReader> _resourceReader;
    std::unique_ptr<future_lite::Executor> _asyncInterExecutor = nullptr;
    std::unique_ptr<future_lite::Executor> _asyncIntraExecutor = nullptr;
};

#define CHECK_TRACE_COUNT(expectNums, filter, traces)                                              \
    ASSERT_NO_FATAL_FAILURE(OpTestBase::checkTraceCount(expectNums, filter, traces))

#define IF_FAILURE_RET_NULL(expr)                                                                  \
    do {                                                                                           \
        auto wrapper = [&]() { ASSERT_NO_FATAL_FAILURE(expr); };                                   \
        wrapper();                                                                                 \
        if (HasFatalFailure()) {                                                                   \
            return nullptr;                                                                        \
        }                                                                                          \
    } while (false)

} // namespace sql
