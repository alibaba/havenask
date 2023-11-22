#include "sql/ops/test/OpTestBase.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <set>
#include <type_traits>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "build_service/config/ResourceReader.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "ha3/common/QueryInfo.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/PathUtil.h"
#include "matchdoc/MatchDoc.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "navi/util/NaviTestPool.h"
#include "sql/common/common.h"
#include "sql/data/SqlQueryConfigData.h"
#include "sql/data/TableData.h"
#include "sql/resource/AnalyzerFactoryR.h"
#include "sql/resource/Ha3QueryInfoR.h"
#include "sql/resource/Ha3TableInfoR.h"
#include "sql/resource/TabletManagerR.h"
#include "sql/resource/testlib/MockTabletManagerR.h"
#include "suez/turing/expression/function/FuncConfig.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/function/FunctionInterfaceCreatorR.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "suez/turing/navi/TableInfoR.h"
#include "suez_navi/resource/AsyncExecutorR.h"

namespace sql {

OpTestBase::OpTestBase()
    : _poolPtr(new autil::mem_pool::PoolAsan())
    , _matchDocUtil(_poolPtr)
    , _tableName("invertedTable") {
    navi::KernelTesterBuilder builder;
}

OpTestBase::~OpTestBase() {}

void OpTestBase::SetUp() {
    TESTBASE::SetUp();
    _testPath = GET_TEMP_DATA_PATH() + "sql_op_index_path/";
    _resourceReader.reset(new build_service::config::ResourceReader(_testPath));
    _indexApp.reset(new indexlib::partition::IndexApplication);
    _id2IndexAppMap[0] = _indexApp;
    if (_needBuildIndex) {
        ASSERT_NO_FATAL_FAILURE(prepareIndex());
    }
    _asyncInterExecutor = std::make_unique<future_lite::executors::SimpleExecutor>(1);
    _asyncIntraExecutor = std::make_unique<future_lite::executors::SimpleExecutor>(1);

    if (_needBuildTablet) {
        ASSERT_NO_FATAL_FAILURE(prepareTablet());
        ASSERT_TRUE(_indexApp->InitByTablet(_tabletMap));
    }
    ASSERT_NO_FATAL_FAILURE(prepareNamedData());
    ASSERT_NO_FATAL_FAILURE(prepareResources());
}

void OpTestBase::TearDown() {
    _indexApp->Close();
    for (const auto &it : _tabletMap) {
        auto tablet = it.second;
        tablet->Close();
    }
    _attributeMap.clear();
    _indexPartitionMap.clear();
    _inputs.clear();
    _outputs.clear();
    TESTBASE::TearDown();
}

int64_t OpTestBase::getRunId() {
    return 0;
}

navi::GraphMemoryPoolR *OpTestBase::getMemPoolResource() {
    auto *graphMemoryPoolR = _naviRHelper.getOrCreateRes<navi::GraphMemoryPoolR>();
    EXPECT_NE(nullptr, graphMemoryPoolR);
    return graphMemoryPoolR;
}

void OpTestBase::prepareIndex() {
    prepareUserIndex();
    // _indexApp.reset(new indexlib::partition::IndexApplication);
    _indexApp->Init(_indexPartitionMap, _joinMap);
}

void OpTestBase::prepareTablet() {
    assert(false);
}

void OpTestBase::prepareUserIndex() {
    if (_needKkvTable) {
        prepareKkvTable();
    } else if (_needKvTable) {
        prepareKvTable();
    } else {
        prepareInvertedTable();
    }
}

void OpTestBase::prepareKkvTable() {
    std::string tableName, fields, attributes, docs, pkeyField, skeyField;
    int64_t ttl;
    prepareKkvTableData(tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
    std::string testPath = _testPath + tableName;
    auto indexPartition = makeIndexPartitionForKkvTable(
        testPath, tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
    std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

void OpTestBase::prepareKkvTableData(std::string &tableName,
                                     std::string &fields,
                                     std::string &pkeyField,
                                     std::string &skeyField,
                                     std::string &attributes,
                                     std::string &docs,
                                     int64_t &ttl) {
    tableName = _tableName;
    fields = "sk:int64;attr2:uint32;trigger_id:int64;name:string;value:string:true";
    pkeyField = "trigger_id";
    skeyField = "sk";
    attributes = "sk;attr2;name;value";
    docs = "cmd=add,sk=0,attr2=0,trigger_id=0,name=str1,value=111 aaa,ts=10000000;"
           "cmd=add,sk=1,attr2=0,trigger_id=0,name=str2,value=222 bbb,ts=20000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=1,name=str3,value=333 ccc,ts=30000000;"
           "cmd=add,sk=1,attr2=2,trigger_id=1,name=str4,value=444 ddd,ts=40000000;"
           "cmd=add,sk=2,attr2=2,trigger_id=1,name=str5,value=555 eee,ts=50000000;"
           "cmd=add,sk=0,attr2=2,trigger_id=2,name=str6,value=666 fff,ts=60000000";
    ttl = std::numeric_limits<int64_t>::max();
}

indexlib::partition::IndexPartitionPtr
OpTestBase::makeIndexPartitionForKkvTable(const std::string &rootPath,
                                          const std::string &tableName,
                                          const std::string &fieldInfos,
                                          const std::string &pkeyName,
                                          const std::string &skeyName,
                                          const std::string &valueInfos,
                                          const std::string &docStrs,
                                          int64_t ttl) {
    indexlib::config::IndexPartitionSchemaPtr schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateKKVSchema(
            tableName, fieldInfos, pkeyName, skeyName, valueInfos);
    assert(schema.get() != nullptr);
    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;

    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, rootPath, docStrs, options, "", true);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

void OpTestBase::prepareKvTable() {
    std::string tableName, fields, attributes, docs, pkeyField;
    bool enableTTL;
    int64_t ttl;
    prepareKvTableData(tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
    std::string testPath = _testPath + tableName;
    auto indexPartition = makeIndexPartitionForKvTable(
        testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
    std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

void OpTestBase::prepareKvTableData(std::string &tableName,
                                    std::string &fields,
                                    std::string &pkeyField,
                                    std::string &attributes,
                                    std::string &docs,
                                    bool &enableTTL,
                                    int64_t &ttl) {
    tableName = _tableName;
    fields = "attr1:uint32;attr2:string:true;pk:uint32";
    attributes = "attr1;attr2";
    docs = "cmd=add,attr1=0,attr2=00 aa,pk=0,ts=5000000;"
           "cmd=add,attr1=1,attr2=11 bb,pk=1,ts=10000000;"
           "cmd=add,attr1=2,attr2=22 cc,pk=2,ts=15000000;"
           "cmd=add,attr1=3,attr2=33 dd,pk=3,ts=20000000";
    pkeyField = "pk";
    enableTTL = false;
    ttl = std::numeric_limits<int64_t>::max();
}

indexlib::partition::IndexPartitionPtr
OpTestBase::makeIndexPartitionForKvTable(const std::string &rootPath,
                                         const std::string &tableName,
                                         const std::string &fieldInfos,
                                         const std::string &pkeyName,
                                         const std::string &valueInfos,
                                         const std::string &docStrs,
                                         bool &enableTTL,
                                         int64_t ttl) {
    indexlib::config::IndexPartitionSchemaPtr schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateKVSchema(
            tableName, fieldInfos, pkeyName, valueInfos, enableTTL);
    assert(schema.get() != nullptr);
    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;

    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, rootPath, docStrs, options, "", true);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

void OpTestBase::prepareInvertedTable() {
    std::string tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs;
    int64_t ttl;
    prepareInvertedTableData(
        tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
    std::string testPath = _testPath + tableName;
    auto indexPartition = makeIndexPartitionForInvertedTable(
        testPath, tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
    std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

void OpTestBase::prepareInvertedTableData(std::string &tableName,
                                          std::string &fields,
                                          std::string &indexes,
                                          std::string &attributes,
                                          std::string &summarys,
                                          std::string &truncateProfileStr,
                                          std::string &docs,
                                          int64_t &ttl) {
    tableName = _tableName;
    fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text";
    indexes = "id:primarykey64:id;index_2:text:index2;name:string:name";
    attributes = "attr1;attr2;id";
    summarys = "name";
    truncateProfileStr = "";
    docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a;"
           "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c;"
           "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop;"
           "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d";
    ttl = std::numeric_limits<int64_t>::max();
}

indexlib::partition::IndexPartitionPtr
OpTestBase::makeIndexPartitionForEmbeddingTable(const std::string &indexPath,
                                                const std::string &tableName,
                                                const std::string &schemaPath,
                                                const std::string &schemaFileName,
                                                const std::string &dataPath,
                                                const std::string &pluginPath,
                                                int64_t ttl) {
    indexlib::config::IndexPartitionSchemaPtr schema = indexlib::config::IndexPartitionSchema::Load(
        indexlib::util::PathUtil::JoinPath(schemaPath, schemaFileName));
    if (schema.get() == nullptr) {
        std::cout << "schema is nullptr" << std::endl;
    }
    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;
    autil::EnvUtil::setEnv("TEST_QUICK_EXIT", "true", 1);
    std::string dataStr;
    indexlib::file_system::FslibWrapper::AtomicLoadE(dataPath, dataStr);

    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, indexPath, dataStr, options, pluginPath, true);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

indexlib::partition::IndexPartitionPtr
OpTestBase::makeIndexPartitionForIndexTable(const std::string &rootPath,
                                            const std::string &tableName,
                                            const std::string &fieldInfos,
                                            const std::string &indexInfos,
                                            const std::string &attrInfos,
                                            const std::string &summaryInfos,
                                            const std::string &docStrs) {
    indexlib::config::IndexPartitionSchemaPtr schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
            tableName, fieldInfos, indexInfos, attrInfos, summaryInfos);
    assert(schema.get() != nullptr);
    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, rootPath, docStrs);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

indexlib::partition::IndexPartitionPtr
OpTestBase::makeIndexPartitionForInvertedTable(const std::string &rootPath,
                                               const std::string &tableName,
                                               const std::string &fields,
                                               const std::string &indexes,
                                               const std::string &attributes,
                                               const std::string &summarys,
                                               const std::string &truncateProfileStr,
                                               const std::string &docs,
                                               int64_t ttl) {
    indexlib::config::IndexPartitionSchemaPtr schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
            tableName, fields, indexes, attributes, summarys, truncateProfileStr);
    assert(schema.get() != nullptr);
    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;
    autil::EnvUtil::setEnv("TEST_QUICK_EXIT", "true", 1);
    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, rootPath, docs, options, "", true);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

void OpTestBase::prepareNamedData() {
    _queryConfigData = std::make_shared<SqlQueryConfigData>();
    _naviRHelper.namedData(SQL_QUERY_CONFIG_NAME, _queryConfigData);
}

void OpTestBase::prepareResources() {
    auto tableInfoR = std::make_shared<suez::turing::TableInfoR>();
    tableInfoR->_id2IndexAppMap = _id2IndexAppMap;
    if (_needBuildIndex || _needBuildTablet) {
        tableInfoR->_tableInfoMapWithoutRel[_tableName] = tableInfoR->_tableInfoWithRel
            = suez::turing::TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    }
    ASSERT_TRUE(_naviRHelper.addExternalRes(tableInfoR));
    if (_needExprResource) {
        auto funcCreatorR = std::make_shared<suez::turing::FunctionInterfaceCreatorR>();
        funcCreatorR->_creator = std::make_shared<suez::turing::FunctionInterfaceCreator>();
        funcCreatorR->_creator->init(suez::turing::FuncConfig(), {});
        ASSERT_TRUE(_naviRHelper.addExternalRes(funcCreatorR));
    }
    auto executorR = std::make_shared<suez_navi::AsyncExecutorR>();
    executorR->_asyncInterExecutor = _asyncInterExecutor.get();
    executorR->_asyncIntraExecutor = _asyncIntraExecutor.get();
    ASSERT_TRUE(_naviRHelper.addExternalRes(executorR));
    auto analyzerR = std::make_shared<AnalyzerFactoryR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(analyzerR));
    auto *queryInfoR = _naviRHelper.getOrCreateRes<Ha3QueryInfoR>();
    ASSERT_TRUE(queryInfoR);
    queryInfoR->_queryInfo.setDefaultIndexName("index_2");
    auto ha3TableInfo = std::make_shared<Ha3TableInfoR>();
    ASSERT_TRUE(_naviRHelper.addExternalRes(ha3TableInfo));
    ASSERT_NO_FATAL_FAILURE(prepareTabletManagerR());
}

void OpTestBase::prepareTabletManagerR() {
    auto mockTabletManagerR = std::make_shared<MockTabletManagerR>();
    auto it = _tabletMap.find(_tableName);
    if (it != _tabletMap.end()) {
        EXPECT_CALL(*mockTabletManagerR, getTablet(_tableName)).WillRepeatedly(Return(it->second));
        EXPECT_CALL(*mockTabletManagerR, waitTabletByTargetTs(_tableName, _, _, _))
            .WillRepeatedly(testing::Invoke([it](std::string tableName,
                                                 int64_t watermark,
                                                 TabletManagerR::CallbackFunc cb,
                                                 int64_t timeoutUs) { cb({it->second}); }));
    }
    ASSERT_TRUE(_naviRHelper.addExternalRes(mockTabletManagerR));
}

void OpTestBase::setResource(navi::KernelTesterBuilder &testerBuilder) {
    testerBuilder.resource(_naviRHelper.getResourceMap());
    testerBuilder.namedData(SQL_QUERY_CONFIG_NAME, _queryConfigData);
}

void OpTestBase::compareStringColumn(table::Table *table,
                                     const std::string &colName,
                                     const std::vector<std::string> &vals) {
    auto col = table->getColumn(colName);
    ASSERT_TRUE(col != nullptr);
    ASSERT_EQ(vals.size(), col->getRowCount());
    auto data = col->getColumnData<autil::MultiChar>();
    ASSERT_TRUE(data != nullptr);
    for (size_t i = 0; i < vals.size(); i++) {
        ASSERT_TRUE(data->get(i) == vals[i])
            << "idx: " << i << ", actual:" << data->get(i) << ", expect: " << vals[i];
    }
}

table::TablePtr OpTestBase::getTable(navi::DataPtr data) {
    TableDataPtr tableData = std::dynamic_pointer_cast<TableData>(data);
    return tableData->getTable();
}

navi::DataPtr OpTestBase::createTable(matchdoc::MatchDocAllocatorPtr allocator,
                                      std::vector<matchdoc::MatchDoc> docs) {
    table::TablePtr ret = table::Table::fromMatchDocs(docs, allocator);
    return std::dynamic_pointer_cast<navi::Data>(TableDataPtr(new TableData(ret)));
}

void OpTestBase::checkTraceCount(size_t expectedNums,
                                 const std::string &filter,
                                 const std::vector<std::string> &traces) {
    size_t count = std::count_if(traces.begin(), traces.end(), [&](auto &text) {
        return text.find(filter) != std::string::npos;
    });
    ASSERT_EQ(expectedNums, count) << "filter: " << filter << std::endl
                                   << "traces: " << autil::StringUtil::toString(traces);
}

void OpTestBase::checkTraceCount(size_t expectedNums,
                                 const std::string &filter,
                                 navi::NaviLoggerProvider &provider) {
    auto traces = provider.getTrace("");
    size_t count = std::count_if(traces.begin(), traces.end(), [&](auto &text) {
        return text.find(filter) != std::string::npos;
    });
    ASSERT_EQ(expectedNums, count) << "filter: " << filter << std::endl
                                   << "traces: " << autil::StringUtil::toString(traces);
}

void OpTestBase::makeConfigCtx(const std::string &jsonStr, navi::KernelConfigContextPtr &ctx) {
    navi::KernelTesterBuilder builder;
    builder.attrs(jsonStr);
    ctx = builder.buildConfigContext();
    ASSERT_NE(nullptr, ctx);
}

void OpTestBase::asyncRunKernel(navi::KernelTester &tester,
                                table::TablePtr &outputTable,
                                bool &eof,
                                int64_t timeout) {
    auto rawTimeout = timeout;
    while (1) {
        ASSERT_LT(0, timeout) << "async run kernel exceed timeout " << rawTimeout << "us";
        ASSERT_FALSE(tester.hasError()) << tester.getErrorMessage();
        if (tester.compute()) {
            navi::DataPtr outputData;
            eof = false;
            if (!tester.getOutput("output0", outputData, eof)) {
                continue;
            }
            ASSERT_TRUE(outputData != nullptr);
            outputTable = getTable(outputData);
            ASSERT_TRUE(outputTable != nullptr);
            break;
        }
        usleep(10 * 1000); // 10ms
        timeout -= 10 * 1000;
    }
    ASSERT_FALSE(tester.hasError());
}

void OpTestBase::asyncRunKernelToEof(navi::KernelTester &tester,
                                     table::TablePtr &outputTable,
                                     int64_t timeout) {
    int64_t rawTimeout = timeout;
    for (bool eof = false; !eof;) {
        ASSERT_LT(0, timeout) << "async run kernel to eof exceed timeout " << rawTimeout << "us";
        autil::ScopedTime2 timer;
        table::TablePtr table;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernel(tester, table, eof, timeout));
        if (outputTable) {
            outputTable->merge(table);
        } else {
            outputTable = table;
        }
        timeout -= timer.done_us();
    }
}

} // namespace sql
