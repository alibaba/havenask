#include "sql/ops/scan/SummaryScanR.h"

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/RangeUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/normal/SearchSummaryDocument.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/TableDistribution.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/scan/AsyncSummaryLookupCallbackCtx.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/scan/SummaryScanR.hpp"
#include "sql/ops/test/OpTestBase.h"
#include "sql/resource/PartitionInfoR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "suez/turing/navi/TableInfoR.h"
#include "table/Row.h"
#include "table/Table.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using namespace navi;

using namespace isearch::search;
using namespace isearch::common;

namespace sql {

class MockAsyncSummaryLookupCallbackCtx : public AsyncSummaryLookupCallbackCtx {
public:
    MockAsyncSummaryLookupCallbackCtx(autil::mem_pool::PoolPtr pool)
        : AsyncSummaryLookupCallbackCtx({}, {}, nullptr, pool, nullptr) {}

public:
    MOCK_METHOD3(start, void(std::vector<docid_t>, size_t, int64_t));
    MOCK_CONST_METHOD0(getResult, const indexlib::index::SearchSummaryDocVec &());
    MOCK_CONST_METHOD0(hasError, bool());
};

class MockSummaryScanRForFill : public SummaryScanR {
public:
    MockSummaryScanRForFill() {}

public:
    MOCK_METHOD0(startLookupCtxs, void());
    MOCK_METHOD1(getSummaryDocs, bool(SearchSummaryDocVecType &));
};

class MockSummaryScanRForScan : public SummaryScanR {
public:
    MockSummaryScanRForScan() {}

public:
    MOCK_METHOD1(fillSummary, bool(const std::vector<matchdoc::MatchDoc> &));
    MOCK_METHOD1(fillAttributes, bool(const std::vector<matchdoc::MatchDoc> &));
    MOCK_METHOD2(doCreateTable,
                 table::TablePtr(const matchdoc::MatchDocAllocatorPtr,
                                 std::vector<matchdoc::MatchDoc>));
};

class MockAttributeExpression : public AttributeExpressionTyped<int32_t> {
public:
    MockAttributeExpression() {}

public:
    MOCK_METHOD2(batchEvaluate, bool(matchdoc::MatchDoc *, uint32_t));
};

class SummaryScanRTest : public OpTestBase {
public:
    SummaryScanRTest() {
        _needBuildIndex = true;
        _needExprResource = true;
        _tableName = "summary";
    }

    void prepareUserIndex() override {
        prepareSummaryTable();
    }

    void prepareSummaryTable() {
        string tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs;
        int64_t ttl;
        {
            prepareSummaryPK64TableData(
                tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                                                                     tableName,
                                                                     fields,
                                                                     indexes,
                                                                     attributes,
                                                                     summarys,
                                                                     truncateProfileStr,
                                                                     docs,
                                                                     ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareExtraSummaryPK64TableData(
                tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                                                                     tableName,
                                                                     fields,
                                                                     indexes,
                                                                     attributes,
                                                                     summarys,
                                                                     truncateProfileStr,
                                                                     docs,
                                                                     ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareSummaryPK128TableData(
                tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                                                                     tableName,
                                                                     fields,
                                                                     indexes,
                                                                     attributes,
                                                                     summarys,
                                                                     truncateProfileStr,
                                                                     docs,
                                                                     ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
    }

    void prepareSummaryPK64TableData(string &tableName,
                                     string &fields,
                                     string &indexes,
                                     string &attributes,
                                     string &summarys,
                                     string &truncateProfileStr,
                                     string &docs,
                                     int64_t &ttl) {
        tableName = "summary";
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;"
                 "price:double;summary1:int32:true;summary2:string:true;";
        indexes = "pk:primarykey64:id";
        attributes = "attr1;attr2;id";
        summarys = "name;price;summary1;summary2;id";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,"
               "price=1.1,summary1=1 1,summary2=aa;"
               "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,"
               "price=2.2,summary1=1 2,summary2=a b c;"
               "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,"
               "price=3.3,summary1=2 1,summary2=a c stop;"
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,"
               "price=4.4,summary1=2 2,summary2=b c d";
        ttl = numeric_limits<int64_t>::max();
    }

    void prepareExtraSummaryPK64TableData(string &tableName,
                                          string &fields,
                                          string &indexes,
                                          string &attributes,
                                          string &summarys,
                                          string &truncateProfileStr,
                                          string &docs,
                                          int64_t &ttl) {
        tableName = "summary_extra";
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;"
                 "price:double;summary1:int32:true;summary2:string:true;";
        indexes = "pk:primarykey64:id";
        attributes = "attr1;attr2;id";
        summarys = "name;price;summary1;summary2;id";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=4,attr2=0 10,id=5,name=ee,"
               "price=1.1,summary1=3 1,summary2=aa;"
               "cmd=add,attr1=5,attr2=1 11,id=6,name=ff,"
               "price=2.2,summary1=3 2,summary2=a b c;"
               "cmd=add,attr1=6,attr2=2 22,id=7,name=gg,"
               "price=3.3,summary1=4 1,summary2=a c stop;"
               "cmd=add,attr1=7,attr2=3 33,id=8,name=hh,"
               "price=4.4,summary1=4 2,summary2=b c d";
        ttl = numeric_limits<int64_t>::max();
    }

    void prepareSummaryPK128TableData(string &tableName,
                                      string &fields,
                                      string &indexes,
                                      string &attributes,
                                      string &summarys,
                                      string &truncateProfileStr,
                                      string &docs,
                                      int64_t &ttl) {
        tableName = "summary2";
        fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;"
                 "price:double;summary1:int32:true;summary2:string:true;";
        indexes = "pk:primarykey128:id";
        attributes = "attr1;attr2;id";
        summarys = "name;price;summary1;summary2";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,"
               "price=1.1,summary1=1 1,summary2=a a a;"
               "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,"
               "price=2.2,summary1=1 2,summary2=a b c;"
               "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,"
               "price=3.3,summary1=2 1,summary2=a c stop;"
               "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,"
               "price=4.4,summary1=2 2,summary2=b c d";
        ttl = numeric_limits<int64_t>::max();
    }

    void buildTableInfo(SummaryScanR &scanR,
                        indexlib::InvertedIndexType pkType,
                        const std::string &field = "",
                        const std::string &index = "") {
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        auto tableInfo = std::make_shared<TableInfo>();
        SummaryInfo *summaryInfo = new SummaryInfo();
        tableInfo->setSummaryInfo(summaryInfo);
        suez::turing::IndexInfos *infos = new suez::turing::IndexInfos();
        suez::turing::IndexInfo *info = new suez::turing::IndexInfo();
        info->indexType = pkType;
        info->fieldName = field;
        info->setIndexName(index.c_str());
        infos->addIndexInfo(info);
        tableInfo->setIndexInfos(infos);
        scanR._attributeExpressionCreatorR->_tableInfo = std::move(tableInfo);
        scanR._summaryInfo = summaryInfo;
    }

    void prepareSummaryScanR(SummaryScanR &scanR,
                             const std::vector<docid_t> &docIds = {0, 1, 2, 3}) {
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        scanR._scanInitParamR->tableName = _tableName;
        scanR._scanInitParamR->hashFields = {"id"};
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._queryMemPoolR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        ASSERT_TRUE(scanR._attributeExpressionCreatorR->initExpressionCreator());
        scanR._tableIdx = {0, 0, 0, 0};
        auto tableInfo = scanR._attributeExpressionCreatorR->_tableInfo;
        scanR._summaryInfo = tableInfo->getSummaryInfo();
        scanR._docIds = docIds;
        scanR.initSummary();
    }

    void addExtraIndex() {
        auto *naviRHelper = getNaviRHelper();
        auto tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[_tableName + "_extra"]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    }

    void prepareEmptyParam() {
        auto *naviRHelper = getNaviRHelper();
        auto calcIPR = std::make_shared<CalcInitParamR>();
        ASSERT_TRUE(naviRHelper->addExternalRes(calcIPR));
        auto scanIPR = std::make_shared<ScanInitParamR>();
        scanIPR->tableName = _tableName;
        scanIPR->calcInitParamR = calcIPR.get();
        ASSERT_TRUE(naviRHelper->addExternalRes(scanIPR));
    }

    void prepareResources(SummaryScanR &scanR) {
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._queryMemPoolR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._functionInterfaceCreatorR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._cavaPluginManagerR));
    }
};

TEST_F(SummaryScanRTest, testInit) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
    paramR->tableName = "summary";
    paramR->hashType = "HASH";
    paramR->usedFields = {"id", "attr1"};
    paramR->hashFields = {"id"};
    paramR->calcInitParamR->conditionJson = "{\"op\":\"IN\", \"params\":[\"$id\", 1, 2]}";
    auto scanR = naviRHelper->getOrCreateRes<SummaryScanR>();
    ASSERT_TRUE(scanR);
    ASSERT_EQ(2, scanR->_attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
    ASSERT_EQ(1, scanR->_tableAttributes.size());
    ASSERT_EQ(1, scanR->_tableAttributes[0].size());
    ASSERT_EQ(2, scanR->_docIds.size());
    ASSERT_NE(nullptr, scanR->_summaryInfo);
}

TEST_F(SummaryScanRTest, testDoBatchScan) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    table::TablePtr inputTable;
    bool eof = false;
    {
        MockSummaryScanRForScan scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));

        EXPECT_CALL(scanR, fillSummary(_)).WillOnce(Return(false));
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.doBatchScan(inputTable, eof));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "fill summary failed", traces);
    }
    {
        MockSummaryScanRForScan scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));

        EXPECT_CALL(scanR, fillSummary(_)).WillOnce(Return(true));
        EXPECT_CALL(scanR, fillAttributes(_)).WillOnce(Return(false));
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.doBatchScan(inputTable, eof));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "fill attribute failed", traces);
    }
    table::TablePtr table(new Table(_poolPtr));
    const std::vector<Row> rows(10);
    table->appendRows(rows);
    {
        MockSummaryScanRForScan scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._calcTableR));

        scanR._seekCount = 0;
        scanR._limit = 20;
        EXPECT_CALL(scanR, fillSummary(_)).WillOnce(Return(true));
        EXPECT_CALL(scanR, fillAttributes(_)).WillOnce(Return(true));
        EXPECT_CALL(scanR, doCreateTable(_, _)).WillOnce(Return(table));
        ASSERT_TRUE(scanR.doBatchScan(inputTable, eof));
        ASSERT_EQ(10, table->getRowCount());
    }
    {
        MockSummaryScanRForScan scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._calcTableR));

        scanR._seekCount = 0;
        scanR._limit = 8;
        EXPECT_CALL(scanR, fillSummary(_)).WillOnce(Return(true));
        EXPECT_CALL(scanR, fillAttributes(_)).WillOnce(Return(true));
        EXPECT_CALL(scanR, doCreateTable(_, _)).WillOnce(Return(table));
        ASSERT_TRUE(scanR.doBatchScan(inputTable, eof));
        ASSERT_EQ(8, table->getRowCount());
    }
}

TEST_F(SummaryScanRTest, testUpdateScanQuery) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
    paramR->tableName = "summary";
    paramR->hashType = "HASH";
    paramR->hashFields = {"id"};
    paramR->tableDist.partCnt = 1;
    auto partitionR = naviRHelper->getOrCreateRes<PartitionInfoR>();
    partitionR->_range.first = 0;
    partitionR->_range.second = 65535;
    MockSummaryScanRForFill scanR;
    ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "id"));
    auto &indexReader = scanR._attributeExpressionCreatorR->_indexPartitionReaderWrapper;
    scanR._indexPartitionReaderWrappers = {indexReader};
    paramR->calcInitParamR->outputFields = {"attr1", "id", "summary1"};
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._calcTableR));
    auto &docIds = scanR._docIds;
    ASSERT_EQ(0, docIds.size());
    {
        navi::NaviLoggerProvider provider("TRACE3");
        EXPECT_CALL(scanR, startLookupCtxs()).WillOnce(Return());
        ASSERT_TRUE(scanR.updateScanQuery({}));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "raw pk is empty for null input query", traces);
    }
    {
        StreamQueryPtr inputQuery(new StreamQuery());
        inputQuery->primaryKeys = {"3", "4"};
        EXPECT_CALL(scanR, startLookupCtxs()).WillOnce(Return());
        ASSERT_TRUE(scanR.updateScanQuery(inputQuery));
        ASSERT_EQ(2, docIds.size());
        ASSERT_EQ(2, docIds[0]);
        ASSERT_EQ(3, docIds[1]);
    }
}

TEST_F(SummaryScanRTest, testParseQuery) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *calcIPR = naviRHelper->getOrCreateRes<CalcInitParamR>();
    {
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        calcIPR->conditionJson = jsonStr;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scanR.parseQuery(pks, needFilter));
        ASSERT_EQ(1, pks.size());
        ASSERT_EQ("1", pks[0]);
        ASSERT_TRUE(needFilter);
    }
    {
        calcIPR->conditionJson = "";
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        vector<string> pks;
        bool needFilter = true;
        ASSERT_FALSE(scanR.parseQuery(pks, needFilter));
    }
    {
        calcIPR->conditionJson = "";
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        scanR._requirePk = false;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scanR.parseQuery(pks, needFilter));
        ASSERT_TRUE(needFilter);
    }
    {
        string condStr1 = "{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 = "{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 + "," + condStr2 + "]}";
        calcIPR->conditionJson = jsonStr;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        vector<string> pks;
        bool needFilter = true;
        ASSERT_FALSE(scanR.parseQuery(pks, needFilter));
    }
    {
        string jsonStr = "{";
        calcIPR->conditionJson = jsonStr;
        vector<string> pks;
        bool needFilter = true;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        ASSERT_FALSE(scanR.parseQuery(pks, needFilter));
    }
    {
        string condStr = "{\"op\":\"IN\", \"params\":[\"$attr1\", 100, 200]}";
        calcIPR->conditionJson = condStr;
        vector<string> pks;
        bool needFilter = true;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        ASSERT_TRUE(scanR.parseQuery(pks, needFilter));
        ASSERT_EQ(2, pks.size());
        ASSERT_EQ("100", pks[0]);
        ASSERT_EQ("200", pks[1]);
        ASSERT_FALSE(needFilter);
    }
    {
        string condStr
            = "{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        calcIPR->conditionJson = condStr;
        vector<string> pks;
        bool needFilter = true;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanR));
        ASSERT_NO_FATAL_FAILURE(buildTableInfo(scanR, it_primarykey64, "attr1", "attr1_idx"));
        ASSERT_TRUE(scanR.parseQuery(pks, needFilter));
        ASSERT_EQ(2, pks.size());
        ASSERT_EQ("100", pks[0]);
        ASSERT_EQ("200", pks[1]);
        ASSERT_FALSE(needFilter);
    }
}

TEST_F(SummaryScanRTest, testPrepareFields) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    {
        SummaryScanR scanR;
        scanR._usedFields = {"attr1", "aaaa"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(2, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScanR scanR;
        scanR._usedFields = {"attr1", "attr1"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(1, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScanR scanR;
        scanR._usedFields = {"summary1", "summary1"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(1, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScanR scanR;
        scanR._usedFields = {"summary1", "attr1", "name"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(3, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScanR scanR;
        scanR._usedFields = {"id"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(1, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
        ASSERT_EQ(1, scanR._tableAttributes.size());
        ASSERT_EQ(0, scanR._tableAttributes[0].size());
    }
    {
        SummaryScanR scanR;
        scanR._usedFields = {"attr1", "attr2", "name", "summary1", "summary2", "id", "price"};
        ASSERT_NO_FATAL_FAILURE(prepareSummaryScanR(scanR));
        ASSERT_TRUE(scanR.prepareFields());
        ASSERT_EQ(7, scanR._attributeExpressionCreatorR->_matchDocAllocator->getReferenceCount());
        ASSERT_EQ(1, scanR._tableAttributes.size());
        ASSERT_EQ(2, scanR._tableAttributes[0].size());
    }
}

TEST_F(SummaryScanRTest, testConvertPK64ToDocId) {
    auto *naviRHelper = getNaviRHelper();
    SummaryScanR summaryScanR;
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary");
    summaryScanR._indexPartitionReaderWrappers = {indexReader};
    vector<string> pks = {"1", "3"};
    ASSERT_TRUE(summaryScanR.convertPK2DocId(pks));
    ASSERT_EQ(2, summaryScanR._docIds.size());
    ASSERT_EQ(0, summaryScanR._docIds[0]);
    ASSERT_EQ(2, summaryScanR._docIds[1]);
}

TEST_F(SummaryScanRTest, testConvertPK128ToDocId) {
    auto *naviRHelper = getNaviRHelper();
    SummaryScanR summaryScanR;
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary2");
    summaryScanR._indexPartitionReaderWrappers = {indexReader};
    vector<string> pks = {"1", "3"};
    ASSERT_TRUE(summaryScanR.convertPK2DocId(pks));
    ASSERT_EQ(2, summaryScanR._docIds.size());
    ASSERT_EQ(0, summaryScanR._docIds[0]);
    ASSERT_EQ(2, summaryScanR._docIds[1]);
}

TEST_F(SummaryScanRTest, testConvertPK64ToDocIdTemp) {
    auto *naviRHelper = getNaviRHelper();
    SummaryScanR summaryScanR;
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary");
    summaryScanR._indexPartitionReaderWrappers = {indexReader};
    std::vector<string> pks = {"2", "4"};
    ASSERT_TRUE(summaryScanR.convertPK2DocId(pks));
    ASSERT_EQ(2, summaryScanR._docIds.size());
    ASSERT_EQ(1, summaryScanR._docIds[0]);
    ASSERT_EQ(3, summaryScanR._docIds[1]);
}

TEST_F(SummaryScanRTest, testConvertPK128ToDocIdTemp) {
    auto *naviRHelper = getNaviRHelper();
    SummaryScanR summaryScanR;
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary2");
    summaryScanR._indexPartitionReaderWrappers = {indexReader};
    std::vector<string> pks = {"2", "4"};
    ASSERT_TRUE(summaryScanR.convertPK2DocId(pks));
    ASSERT_EQ(2, summaryScanR._docIds.size());
    ASSERT_EQ(1, summaryScanR._docIds[0]);
    ASSERT_EQ(3, summaryScanR._docIds[1]);
}

TEST_F(SummaryScanRTest, testFillSummaryDocs) {
    SummaryScanR scanR;
    auto *naviRHelper = getNaviRHelper();
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._queryMemPoolR));
    int32_t summaryFieldId = 0;
    {
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.fillSummaryDocs<int32_t>(NULL, {}, {}, summaryFieldId));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "reference is empty", traces);
    }
    std::vector<indexlib::document::SearchSummaryDocument *> summaryDocs;
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), false));
    auto ref = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
    std::vector<docid_t> docIds = {0};
    std::vector<matchdoc::MatchDoc> matchDocs = allocator->batchAllocate(docIds);
    {
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.fillSummaryDocs(ref, summaryDocs, matchDocs, summaryFieldId));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "size not equal left", traces);
    }
    indexlib::document::SearchSummaryDocument summaryDoc(_poolPtr.get(), 1);
    summaryDocs.push_back(&summaryDoc);
    {
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.fillSummaryDocs(ref, summaryDocs, matchDocs, -1));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "summary get field id", traces);
    }
    {
        const string value = "";
        summaryDoc.SetFieldValue(summaryFieldId, value.data(), value.size(), false);
        ASSERT_TRUE(scanR.fillSummaryDocs(ref, summaryDocs, matchDocs, summaryFieldId));
        ASSERT_EQ(0, ref->get(matchDocs[summaryFieldId]));
    }
    {
        const string value = "12";
        summaryDoc.SetFieldValue(summaryFieldId, value.data(), value.size(), false);
        ASSERT_TRUE(scanR.fillSummaryDocs(ref, summaryDocs, matchDocs, summaryFieldId));
        ASSERT_EQ(12, ref->get(matchDocs[summaryFieldId]));
    }
}

TEST_F(SummaryScanRTest, testFillSummary) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
    paramR->tableName = _tableName;
    TableInfoPtr tableInfo(new TableInfo());
    auto summaryInfo = std::make_shared<SummaryInfo>();
    {
        MockSummaryScanRForFill scanR;
        EXPECT_CALL(scanR, getSummaryDocs(_)).WillOnce(Return(false));
        ASSERT_FALSE(scanR.fillSummary({}));
    }
    {
        MockSummaryScanRForFill scanR;
        scanR._summaryInfo = summaryInfo.get();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        // field count empty
        EXPECT_CALL(scanR, getSummaryDocs(_)).WillOnce(Return(true));
        ASSERT_TRUE(scanR.fillSummary({}));
    }
    summaryInfo->addFieldName("aa");
    {
        // field not match
        MockSummaryScanRForFill scanR;
        scanR._summaryInfo = summaryInfo.get();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        auto &allocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        auto ref = allocator->declare<int32_t>("bb", SL_ATTRIBUTE);
        std::vector<docid_t> docIds = {0};
        std::vector<matchdoc::MatchDoc> matchDocs = allocator->batchAllocate(docIds);
        EXPECT_CALL(scanR, getSummaryDocs(_)).WillOnce(Return(true));
        ASSERT_TRUE(scanR.fillSummary(matchDocs));
        ASSERT_EQ(0, ref->get(matchDocs[0]));
    }
    {
        // fillSummaryDocs return false
        MockSummaryScanRForFill scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        scanR._summaryInfo = summaryInfo.get();
        auto &allocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        auto ref = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        std::vector<docid_t> docIds = {0};
        std::vector<matchdoc::MatchDoc> matchDocs = allocator->batchAllocate(docIds);
        std::vector<indexlib::document::SearchSummaryDocument *> summaryDocs;
        EXPECT_CALL(scanR, getSummaryDocs(_))
            .WillOnce(DoAll(SetArgReferee<0>(summaryDocs), Return(true)));
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(scanR.fillSummary(matchDocs));
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "fill summary docs column", traces);
        ASSERT_EQ(0, ref->get(matchDocs[0]));
    }
    {
        MockSummaryScanRForFill scanR;
        scanR._summaryInfo = summaryInfo.get();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._queryMemPoolR));
        auto &allocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        auto ref = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        std::vector<docid_t> docIds = {0};
        std::vector<matchdoc::MatchDoc> matchDocs = allocator->batchAllocate(docIds);
        std::vector<indexlib::document::SearchSummaryDocument *> summaryDocs;
        indexlib::document::SearchSummaryDocument summaryDoc(_poolPtr.get(), 1);
        summaryDocs.push_back(&summaryDoc);
        const string value = "12";
        summaryDoc.SetFieldValue(0, value.data(), value.size(), false);
        EXPECT_CALL(scanR, getSummaryDocs(_))
            .WillOnce(DoAll(SetArgReferee<0>(summaryDocs), Return(true)));
        ASSERT_TRUE(scanR.fillSummary(matchDocs));
        ASSERT_EQ(12, ref->get(matchDocs[0]));
    }
}

TEST_F(SummaryScanRTest, testFillAttributes) {
    SummaryScanR scanR;
    MockAttributeExpression attr;
    scanR._tableAttributes.resize(1);
    scanR._tableAttributes[0].push_back(&attr);
    {
        EXPECT_CALL(attr, batchEvaluate(_, _)).WillOnce(Return(false));
        ASSERT_FALSE(scanR.fillAttributes({}));
    }
    {
        EXPECT_CALL(attr, batchEvaluate(_, _)).WillOnce(Return(true));
        ASSERT_TRUE(scanR.fillAttributes({}));
    }
}

TEST_F(SummaryScanRTest, testCreateTable) {
    auto *naviRHelper = getNaviRHelper();
    { // reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), false));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._graphMemoryPoolR));
        TablePtr table = scanR.createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
    { // copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._graphMemoryPoolR));
        TablePtr table = scanR.createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
}

TEST_F(SummaryScanRTest, testInitExtraSummary) {
    addExtraIndex();
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
    {
        SummaryScanR scanR;
        paramR->tableName = "summary";
        ASSERT_NO_FATAL_FAILURE(prepareResources(scanR));
        scanR.initExtraSummary();
        ASSERT_EQ(1, scanR._indexPartitionReaderWrappers.size());
        ASSERT_EQ(1, scanR._attributeExpressionCreators.size());
    }
    {
        SummaryScanR scanR;
        paramR->tableName = "summary2";
        ASSERT_NO_FATAL_FAILURE(prepareResources(scanR));
        scanR.initExtraSummary();
        ASSERT_EQ(0, scanR._indexPartitionReaderWrappers.size());
        ASSERT_EQ(0, scanR._attributeExpressionCreators.size());
    }
}

TEST_F(SummaryScanRTest, testMultiTableConvertPK2DocId) {
    SummaryScanR summaryScanR;
    vector<string> pks = {"1", "3", "5", "10"};
    auto *naviRHelper = getNaviRHelper();
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary");
    auto indexReaderExtra
        = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            scanR->partitionReaderSnapshot.get(), "summary_extra");
    summaryScanR._indexPartitionReaderWrappers = {indexReader, indexReaderExtra};
    ASSERT_TRUE(summaryScanR.convertPK2DocId(pks));
    ASSERT_EQ(3, summaryScanR._docIds.size());
    ASSERT_EQ(0, summaryScanR._docIds[0]);
    ASSERT_EQ(2, summaryScanR._docIds[1]);
    ASSERT_EQ(0, summaryScanR._docIds[2]);
    ASSERT_EQ(3, summaryScanR._tableIdx.size());
    ASSERT_EQ(0, summaryScanR._tableIdx[0]);
    ASSERT_EQ(0, summaryScanR._tableIdx[1]);
    ASSERT_EQ(1, summaryScanR._tableIdx[2]);
}

TEST_F(SummaryScanRTest, testPrepareLookupCtxs) {
    SummaryScanR summaryScanR;
    auto *naviRHelper = getNaviRHelper();
    auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
    ASSERT_TRUE(scanR);
    auto indexReader = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
        scanR->partitionReaderSnapshot.get(), "summary");
    auto indexReaderExtra
        = isearch::search::IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            scanR->partitionReaderSnapshot.get(), "summary_extra");
    summaryScanR._indexPartitionReaderWrappers = {indexReader, indexReaderExtra};
    ASSERT_TRUE(naviRHelper->getOrCreateRes(summaryScanR._queryMemPoolR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes(summaryScanR._asyncExecutorR));
    ASSERT_TRUE(summaryScanR.prepareLookupCtxs());
    ASSERT_EQ(2, summaryScanR._lookupCtxs.size());
}

TEST_F(SummaryScanRTest, testStartLookupCtxs) {
    SummaryScanR scanR;
    auto *naviRHelper = getNaviRHelper();
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._timeoutTerminatorR));
    auto ctx1 = new MockAsyncSummaryLookupCallbackCtx(_poolPtr);
    auto ctx2 = new MockAsyncSummaryLookupCallbackCtx(_poolPtr);
    scanR._countedPipe.reset(new CountedAsyncPipe({}, 100));
    scanR._countedPipe->_count = 1;
    scanR._lookupCtxs.emplace_back(ctx1);
    scanR._lookupCtxs.emplace_back(ctx2);
    scanR._docIds = {1, 2};
    scanR._tableIdx = {0, 1};
    vector<docid_t> docIds1, docIds2;
    int64_t timeout1, timeout2;
    size_t fieldCount1, fieldCount2;
    SummaryInfo summaryInfo;
    scanR._summaryInfo = &summaryInfo;
    EXPECT_CALL(*ctx1, start(_, _, _))
        .WillOnce(DoAll(SaveArg<0>(&docIds1), SaveArg<1>(&fieldCount1), SaveArg<2>(&timeout1)));
    EXPECT_CALL(*ctx2, start(_, _, _))
        .WillOnce(DoAll(SaveArg<0>(&docIds2), SaveArg<1>(&fieldCount2), SaveArg<2>(&timeout2)));
    scanR.startLookupCtxs();
    ASSERT_EQ(100, scanR._countedPipe->_count);
    ASSERT_EQ(1, docIds1.size());
    ASSERT_EQ(1, docIds1[0]);
    ASSERT_EQ(1, docIds2.size());
    ASSERT_EQ(2, docIds2[0]);
    ASSERT_EQ(0, fieldCount1);
    ASSERT_EQ(0, fieldCount2);
    auto leftTime = scanR._timeoutTerminatorR->getTimeoutTerminator()->getLeftTime();
    ASSERT_EQ(timeout1, timeout2);
    ASSERT_TRUE(timeout1 >= leftTime);
}

TEST_F(SummaryScanRTest, testGetSummaryDocs) {
    SummaryScanR scanR;
    auto ctx1 = new MockAsyncSummaryLookupCallbackCtx(_poolPtr);
    auto ctx2 = new MockAsyncSummaryLookupCallbackCtx(_poolPtr);
    scanR._lookupCtxs.emplace_back(ctx1);
    scanR._lookupCtxs.emplace_back(ctx2);
    scanR._docIds = {1, 2};
    scanR._tableIdx = {0, 1};
    indexlib::document::SearchSummaryDocument d1(NULL, 0);
    indexlib::document::SearchSummaryDocument d2(NULL, 0);
    indexlib::index::SearchSummaryDocVec vec1 = {&d1};
    indexlib::index::SearchSummaryDocVec vec2 = {&d2};
    EXPECT_CALL(*ctx1, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*ctx1, getResult()).WillOnce(ReturnRef(vec1));
    EXPECT_CALL(*ctx2, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*ctx2, getResult()).WillOnce(ReturnRef(vec2));
    std::vector<indexlib::document::SearchSummaryDocument *> summaryDocs;
    ASSERT_TRUE(scanR.getSummaryDocs(summaryDocs));
    ASSERT_EQ(2, summaryDocs.size());
    ASSERT_EQ(&d1, summaryDocs[0]);
    ASSERT_EQ(&d2, summaryDocs[1]);
}

TEST_F(SummaryScanRTest, testGetSummaryDocsFailed) {
    SummaryScanR scanR;
    auto ctx = new MockAsyncSummaryLookupCallbackCtx(_poolPtr);
    scanR._lookupCtxs.emplace_back(ctx);
    EXPECT_CALL(*ctx, hasError()).WillOnce(Return(true));
    std::vector<indexlib::document::SearchSummaryDocument *> summaryDocs;
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scanR.getSummaryDocs(summaryDocs));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "summary async scan has error", traces);
}

TEST_F(SummaryScanRTest, testAdjustUsedField) {
    ASSERT_NO_FATAL_FAILURE(prepareEmptyParam());
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<CalcInitParamR>();
    {
        vector<string> expectFields = {"attr2", "attr1"};
        paramR->outputFields = expectFields;
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        scanR._usedFields = {"attr1", "attr2"};
        scanR.adjustUsedFields();
        ASSERT_EQ(expectFields, scanR._usedFields);
    }
    {
        vector<string> expectFields = {"attr1", "attr2"};
        paramR->outputFields = {"attr2", "attr3"};
        SummaryScanR scanR;
        scanR._usedFields = expectFields;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        scanR.adjustUsedFields();
        ASSERT_EQ(expectFields, scanR._usedFields);
    }
    {
        vector<string> expectFields = {"attr1", "attr2"};
        paramR->outputFields = {"attr1", "attr2", "attr3"};
        SummaryScanR scanR;
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._scanInitParamR));
        scanR._usedFields = expectFields;
        scanR.adjustUsedFields();
        ASSERT_EQ(expectFields, scanR._usedFields);
    }
}

} // namespace sql
