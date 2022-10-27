#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/scan/SummaryScan.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <autil/DefaultHashFunction.h>
#include <suez/turing/expression/util/TableInfo.h>
#include <navi/resource/MemoryPoolResource.h>
#include <indexlib/util/hash_string.h>
#include <autil/HashFuncFactory.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <matchdoc/MatchDocAllocator.h>
using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using autil::mem_pool::Pool;
using namespace navi;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class SummaryScanTest : public OpTestBase {
public:
    SummaryScanTest() {
        _tableName = "summary";
    }
    void setUp();
    void tearDown();
    void prepareUserIndex() override {
        prepareSummaryTable();
    }

    void addSqlSessionResource() {
        _sqlResource->range.set_from(0);
        _sqlResource->range.set_to(65535);
    }
    void prepareSummaryTable() {
        string tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs;
        int64_t ttl;
        {
            prepareSummaryPK64TableData(tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                    tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _bizInfo->_itemTableName = schemaName;
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareExtraSummaryPK64TableData(tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                    tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _bizInfo->_itemTableName = schemaName;
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareSummaryPK128TableData(tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForInvertedTable(testPath,
                    tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs, ttl);
            string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
    }

    void prepareSummaryPK64TableData(string &tableName,
            string &fields, string &indexes, string &attributes,
            string &summarys, string &truncateProfileStr,
            string &docs, int64_t &ttl)
    {
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
            string &fields, string &indexes, string &attributes,
            string &summarys, string &truncateProfileStr,
            string &docs, int64_t &ttl)
    {
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
            string &fields, string &indexes, string &attributes,
            string &summarys, string &truncateProfileStr,
            string &docs, int64_t &ttl)
    {
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

    void prepareIndexPartitionReader() {
        _indexPartitionReaderWrapper =
            IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                    _sqlQueryResource->getPartitionReaderSnapshot(), "summary");
    }
    void prepareExtraIndexPartitionReader() {
        _extraIndexPartitionReaderWrapper =
            IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                    _sqlQueryResource->getPartitionReaderSnapshot(), "summary_extra");
    }
    void prepareIndexPartitionReaderForPK128() {
        _indexPartitionReaderWrapper2 =
            IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                    _sqlQueryResource->getPartitionReaderSnapshot(), "summary2");
    }
    void buildTableInfo(SummaryScan &scan, ::IndexType pkType,
                        const std::string &field = "",
                        const std::string &index = "")
    {
        scan._tableInfo.reset(new TableInfo());
        suez::turing::IndexInfos *infos = new suez::turing::IndexInfos();
        suez::turing::IndexInfo *info = new suez::turing::IndexInfo();
        info->indexType = pkType;
        info->fieldName = field;
        info->setIndexName(index.c_str());
        infos->addIndexInfo(info);
        scan._tableInfo->setIndexInfos(infos);
    }

    void prepareSummaryScan(SummaryScan &scan,
                            const std::vector<docid_t> &docIds = {0, 1, 2, 3})
    {
        prepareIndexPartitionReader();
        scan._tableIdx = {0, 0, 0, 0};
        scan._indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
        scan._tableName = _tableName;
        scan._pool = &_pool;
        scan._memoryPoolResource = &_memPoolResource;
        scan.initExpressionCreator(_sessionResource->tableInfo, false,
                                   _sqlBizResource.get(), _sqlQueryResource.get(), SCAN_SUMMARY_TABLE_TYPE);
        scan._tableInfo = _sessionResource->tableInfo;
        scan._docIds = docIds;
        scan._hashFields = {"id"};
        scan.initSummary();
    }

    void fetchAndCheckFields(SummaryScan &scan,
                             const std::vector<std::string> &usedFields)
    {
        scan._usedFields = usedFields;
        scan.initSummary();
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_TRUE(scan.prepareSummaryReader());
        auto docs = scan._matchDocAllocator->batchAllocate(scan._docIds);
        TablePtr table(new Table(docs, scan._matchDocAllocator));
        ASSERT_TRUE(scan.fillSummary(docs));
        ASSERT_TRUE(scan.fillAttributes(docs));
        for (auto &field : scan._usedFields) {
            if (field == "summary1") {
                ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(table, "summary1",
                                {{1, 1}, {1, 2}, {2, 1}, {2, 2}}));
            } else if (field == "summary2") {
                ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<string>(table, "summary2",
                                {
                                    {"a", "", "a"},
                                    {"a", "b", "c"},
                                    {"a", "c", "stop"},
                                    {"b", "c", "d"}
                                }));
            } else if (field == "price") {
                ASSERT_NO_FATAL_FAILURE(checkOutputColumn<double>(table, "price", {1.1, 2.2, 3.3, 4.4}));
            } else if (field == "name") {
                ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(table, "name", {"aa", "bb", "cc", "dd"}));
            } else if (field == "attr1") {
                ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(table, "attr1", {0, 1, 2, 3}));
            } else if (field == "attr2") {
                ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(
                                table, "attr2", {{0, 10}, {1, 11}, {2, 22}, {3, 33}}));
            } else if (field == "id") {
                ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "id", {1, 2, 3, 4}));
            } else {
                ASSERT_NO_FATAL_FAILURE(checkOutputColumn<string>(table, field, {"null", "null", "null", "null"}));
            }
        }
    }

    void addExtraIndex() {
        _sessionResource->dependencyTableInfoMap[_tableName + "_extra"] =
            suez::turing::TableInfoConfigurator::createFromIndexApp(_bizInfo->_itemTableName, _indexApp);
    }

private:
    HA3_LOG_DECLARE();
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    search::IndexPartitionReaderWrapperPtr _extraIndexPartitionReaderWrapper;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper2;
};

HA3_LOG_SETUP(searcher, SummaryScanTest);

void SummaryScanTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = true;
}

void SummaryScanTest::tearDown() {
}

TEST_F(SummaryScanTest, testInit) {
    addSqlSessionResource();
    ScanInitParam param;
    param.tableName = "summary";
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    MemoryPoolResource memoryPoolResource;
    param.memoryPoolResource = &memoryPoolResource;
    param.hashType = "HASH";
    param.usedFields = {"id", "attr1"};
    param.hashFields = {"id"};
    param.conditionJson ="{\"op\":\"IN\", \"params\":[\"$id\", 1, 2]}";
    SummaryScan scan;
    ASSERT_TRUE(scan.init(param));
    ASSERT_EQ(2, scan._matchDocAllocator->getReferenceCount());
    ASSERT_EQ(1, scan._tableAttributes.size());
    ASSERT_EQ(2, scan._tableAttributes[0].size());
    ASSERT_EQ(2, scan._docIds.size());
}

TEST_F(SummaryScanTest, testDoBatchScan) {
    SummaryScan scan;
    prepareSummaryScan(scan);
    scan._usedFields = {"attr1", "id", "summary1"};
    ASSERT_TRUE(scan.prepareFields());
    ASSERT_TRUE(scan.prepareSummaryReader());
    scan._calcTable.reset(new CalcTable(&_pool, &_memPoolResource,
                    {"attr1", "id", "summary1"},{}, NULL, NULL,
                    suez::turing::CavaPluginManagerPtr(), NULL));
    TablePtr table;
    bool eof;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(4, table->getRowCount());
    ASSERT_EQ(3, table->getColumnCount());
}

TEST_F(SummaryScanTest, testUpdateScanQuery) {
    addSqlSessionResource();
    ScanInitParam param;
    param.tableName = "summary";
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    MemoryPoolResource memoryPoolResource;
    param.memoryPoolResource = &memoryPoolResource;
    param.hashType = "HASH";
    param.usedFields = {"id", "attr1"};
    param.hashFields = {"id"};
    param.outputFields = {"id", "attr1"};
    param.conditionJson ="{\"op\":\"IN\", \"params\":[\"$id\", 1, 2, 3]}";
    SummaryScan scan;
    scan._tableIdx = {0, 0, 0, 0};
    ASSERT_TRUE(scan.init(param));
    ASSERT_EQ(2, scan._matchDocAllocator->getReferenceCount());
    ASSERT_EQ(1, scan._tableAttributes.size());
    ASSERT_EQ(2, scan._tableAttributes[0].size());
    ASSERT_EQ(3, scan._docIds.size());
    StreamQueryPtr inputQuery(new StreamQuery());
    QueryPtr query(new TermQuery("3", "pk", RequiredFields(), ""));
    inputQuery->query = query;
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    TablePtr table;
    bool eof;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("3", table->toString(0, 0));
    ASSERT_EQ("2", table->toString(0, 1));
    ASSERT_EQ(1, scan._seekCount);

    inputQuery->query.reset(new TermQuery("2", "pk", RequiredFields(), ""));
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    eof = false;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("2", table->toString(0, 0));
    ASSERT_EQ("1", table->toString(0, 1));
    ASSERT_EQ(2, scan._seekCount);

    inputQuery->query.reset(new TermQuery("4", "pk", RequiredFields(), ""));
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    eof = false;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_EQ(2, scan._seekCount);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, table->getRowCount());
}

TEST_F(SummaryScanTest, testUpdateScanQueryWithLimit1) {
    addSqlSessionResource();
    ScanInitParam param;
    param.tableName = "summary";
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    MemoryPoolResource memoryPoolResource;
    param.memoryPoolResource = &memoryPoolResource;
    param.hashType = "HASH";
    param.usedFields = {"id", "attr1"};
    param.hashFields = {"id"};
    param.outputFields = {"id", "attr1"};
    param.conditionJson ="{\"op\":\"IN\", \"params\":[\"$id\", 1, 2, 3]}";
    param.limit = 1;
    SummaryScan scan;
    scan._tableIdx = {0, 0, 0, 0};
    ASSERT_TRUE(scan.init(param));
    ASSERT_EQ(2, scan._matchDocAllocator->getReferenceCount());
    ASSERT_EQ(1, scan._tableAttributes.size());
    ASSERT_EQ(2, scan._tableAttributes[0].size());
    ASSERT_EQ(3, scan._docIds.size());
    StreamQueryPtr inputQuery(new StreamQuery());
    QueryPtr query(new TermQuery("3", "pk", RequiredFields(), ""));
    inputQuery->query = query;
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    TablePtr table;
    bool eof;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("3", table->toString(0, 0));
    ASSERT_EQ("2", table->toString(0, 1));
    ASSERT_EQ(1, scan._seekCount);

    inputQuery->query.reset(new TermQuery("2", "pk", RequiredFields(), ""));
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    eof = false;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ(2, scan._seekCount);

    inputQuery->query.reset(new TermQuery("4", "pk", RequiredFields(), ""));
    ASSERT_TRUE(scan.updateScanQuery(inputQuery));
    ASSERT_EQ(1, scan._docIds.size());
    eof = false;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_EQ(2, scan._seekCount);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, table->getRowCount());
}


TEST_F(SummaryScanTest, testDoBatchScanFillSummaryFailed) {
    SummaryScan scan;
    prepareSummaryScan(scan, {10});
    scan._usedFields = {"attr1", "id", "summary1"};
    ASSERT_TRUE(scan.prepareFields());
    ASSERT_TRUE(scan.prepareSummaryReader());
    scan._calcTable.reset(new CalcTable(&_pool, &_memPoolResource,
                    {"attr1", "id", "summary1"}, {},  NULL, NULL,
                    suez::turing::CavaPluginManagerPtr(), NULL));
    TablePtr table;
    bool eof;
    ASSERT_FALSE(scan.doBatchScan(table, eof));
}

TEST_F(SummaryScanTest, testDoBatchScanFillAttributesFailed) {
    SummaryScan scan;
    prepareSummaryScan(scan);
    scan._usedFields = {"attr1", "id", "summary1"};
    ASSERT_TRUE(scan.prepareFields());
    ASSERT_TRUE(scan.prepareSummaryReader());
    AttributeExpressionPtr expr(new AttributeExpressionTyped<int32_t>());
    scan._tableAttributes.push_back({expr.get()});
    scan._calcTable.reset(new CalcTable(&_pool, &_memPoolResource,
                    {"attr1", "id", "summary1"}, {}, NULL, NULL,
                    suez::turing::CavaPluginManagerPtr(), NULL));
    TablePtr table;
    bool eof;
    ASSERT_FALSE(scan.doBatchScan(table, eof));
}

TEST_F(SummaryScanTest, testGenRawDocIdFromQuery) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey64, "attr1", "attr1");
    {
        vector<string> rawPks;
        QueryPtr query(new TermQuery("123", "attr1",  RequiredFields(), ""));
        ASSERT_TRUE(scan.genRawDocIdFromQuery(query, rawPks));
        ASSERT_EQ(1, rawPks.size());
        ASSERT_EQ("123", rawPks[0]);
    }
    {
        vector<string> rawPks;
        MultiTermQuery *multiQuery = new MultiTermQuery("");
        multiQuery->addTerm(TermPtr(new Term("123", "attr1", RequiredFields())));
        multiQuery->addTerm(TermPtr(new Term("1234", "attr1",  RequiredFields())));
        QueryPtr query(multiQuery);
        ASSERT_TRUE(scan.genRawDocIdFromQuery(query, rawPks));
        ASSERT_EQ(2, rawPks.size());
        ASSERT_EQ("123", rawPks[0]);
        ASSERT_EQ("1234", rawPks[1]);
    }
    {
        vector<string> rawPks;
        QueryPtr query(new TermQuery("123", "attr", RequiredFields(), ""));
        ASSERT_TRUE(scan.genRawDocIdFromQuery(query, rawPks));
    }
    {
        vector<string> rawPks;
        MultiTermQuery *multiQuery = new MultiTermQuery("");
        multiQuery->addTerm(TermPtr(new Term("123", "attr", RequiredFields())));
        multiQuery->addTerm(TermPtr(new Term("1234", "attr1", RequiredFields())));
        QueryPtr query(multiQuery);
        ASSERT_TRUE(scan.genRawDocIdFromQuery(query, rawPks));
    }
    {
        vector<string> rawPks;
        QueryPtr query(new AndQuery(""));
        ASSERT_TRUE(scan.genRawDocIdFromQuery(query, rawPks));
    }
}


TEST_F(SummaryScanTest, testParseQuery) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey64, "attr1", "attr1_idx");
    {
        string condStr1 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 ="{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        scan._conditionJson = jsonStr;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scan.parseQuery(pks, needFilter));
        ASSERT_EQ(1, pks.size());
        ASSERT_EQ("1", pks[0]);
        ASSERT_TRUE(needFilter);
    }
    {
        scan._conditionJson = "";
        vector<string> pks;
        bool needFilter = true;
        ASSERT_FALSE(scan.parseQuery(pks, needFilter));
    }
    {
        SummaryScan scan2(false);
        scan2._conditionJson = "";
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scan2.parseQuery(pks, needFilter));
        ASSERT_TRUE(needFilter);
    }
    {
        string condStr1 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 ="{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        scan._conditionJson = jsonStr;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_FALSE(scan.parseQuery(pks, needFilter));
    }
    {
        string condStr1 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 ="{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        scan._conditionJson = jsonStr;
        vector<string> pks;
        SummaryScan scan2(false);
        bool needFilter = true;
        ASSERT_TRUE(scan2.parseQuery(pks, needFilter));
        ASSERT_TRUE(needFilter);
    }
    {
        string jsonStr ="{";
        scan._conditionJson = jsonStr;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_FALSE(scan.parseQuery(pks, needFilter));
    }
    {
        string condStr ="{\"op\":\"IN\", \"params\":[\"$attr1\", 100, 200]}";
        scan._conditionJson = condStr;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scan.parseQuery(pks, needFilter));
        ASSERT_EQ(2, pks.size());
        ASSERT_EQ("100", pks[0]);
        ASSERT_EQ("200", pks[1]);
        ASSERT_FALSE(needFilter);
    }
    {
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"100|200\"],\"type\":\"UDF\"}";
        scan._conditionJson = condStr;
        vector<string> pks;
        bool needFilter = true;
        ASSERT_TRUE(scan.parseQuery(pks, needFilter));
        ASSERT_EQ(2, pks.size());
        ASSERT_EQ("100", pks[0]);
        ASSERT_EQ("200", pks[1]);
        ASSERT_FALSE(needFilter);
    }
}

TEST_F(SummaryScanTest, testCalculatePrimaryKey) {
    string rawPk("123");
    SummaryScan scan;
    {
        buildTableInfo(scan, it_primarykey64);
        uint64_t t1;
        IE_NAMESPACE(util)::HashString::Hash(t1, rawPk.data(), rawPk.size());
        ASSERT_EQ(t1, scan.calculatePrimaryKey(rawPk).value[1]);
    }
    {
        buildTableInfo(scan, it_primarykey128);
        autil::uint128_t t2;
        IE_NAMESPACE(util)::HashString::Hash(t2, rawPk.data(), rawPk.size());
        ASSERT_EQ(t2, scan.calculatePrimaryKey(rawPk));
    }
}

TEST_F(SummaryScanTest, testPrepareFields) {
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"attr1", "aaaa"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(2, scan._matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"attr1", "attr1"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(1, scan._matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"summary1", "summary1"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(1, scan._matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"summary1", "attr1", "name"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(3, scan._matchDocAllocator->getReferenceCount());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"id"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(1, scan._matchDocAllocator->getReferenceCount());
        ASSERT_EQ(1, scan._tableAttributes.size());
        ASSERT_EQ(1, scan._tableAttributes[0].size());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._usedFields = {"attr1", "attr2", "name", "summary1", "summary2",
                            "id", "price"};
        ASSERT_TRUE(scan.prepareFields());
        ASSERT_EQ(7, scan._matchDocAllocator->getReferenceCount());
        ASSERT_EQ(1, scan._tableAttributes.size());
        ASSERT_EQ(3, scan._tableAttributes[0].size());
    }
}

TEST_F(SummaryScanTest, testConvertPK64ToDocId) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey64);
    vector<primarykey_t> pks = {scan.calculatePrimaryKey("1"),
                                scan.calculatePrimaryKey("3")};
    prepareIndexPartitionReader();
    scan._indexPartitionReaderWrappers = {_indexPartitionReaderWrapper};
    ASSERT_TRUE(scan.convertPK2DocId(pks));
    ASSERT_EQ(2, scan._docIds.size());
    ASSERT_EQ(0, scan._docIds[0]);
    ASSERT_EQ(2, scan._docIds[1]);
}

TEST_F(SummaryScanTest, testConvertPK128ToDocId) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey128);
    vector<primarykey_t> pks = {scan.calculatePrimaryKey("1"),
                                scan.calculatePrimaryKey("3")};
    prepareIndexPartitionReaderForPK128();
    scan._indexPartitionReaderWrappers = {_indexPartitionReaderWrapper2};
    ASSERT_TRUE(scan.convertPK2DocId(pks));
    ASSERT_EQ(2, scan._docIds.size());
    ASSERT_EQ(0, scan._docIds[0]);
    ASSERT_EQ(2, scan._docIds[1]);
}


TEST_F(SummaryScanTest, testConvertPK64ToDocIdTemp) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey64);
    prepareIndexPartitionReader();
    auto primaryKeyReaderPtr =
        _indexPartitionReaderWrapper->getReader()->GetPrimaryKeyReader().get();
    std::vector<primarykey_t> pks = {scan.calculatePrimaryKey("2"),
                                     scan.calculatePrimaryKey("4")};
    ASSERT_TRUE(scan.convertPK2DocId<uint64_t>(pks, {primaryKeyReaderPtr}));
    ASSERT_EQ(2, scan._docIds.size());
    ASSERT_EQ(1, scan._docIds[0]);
    ASSERT_EQ(3, scan._docIds[1]);
}

TEST_F(SummaryScanTest, testConvertPK128ToDocIdTemp) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey128);
    prepareIndexPartitionReaderForPK128();
    auto primaryKeyReaderPtr =
        _indexPartitionReaderWrapper2->getReader()->GetPrimaryKeyReader().get();
    std::vector<primarykey_t> pks = {scan.calculatePrimaryKey("2"),
                                     scan.calculatePrimaryKey("4")};
    ASSERT_TRUE(scan.convertPK2DocId<uint128_t>(pks, {primaryKeyReaderPtr}));
    ASSERT_EQ(2, scan._docIds.size());
    ASSERT_EQ(1, scan._docIds[0]);
    ASSERT_EQ(3, scan._docIds[1]);
}

TEST_F(SummaryScanTest, testConvertPK128ToDocIdTempFailed) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey128);
    prepareIndexPartitionReaderForPK128();
    auto primaryKeyReaderPtr =
        _indexPartitionReaderWrapper2->getReader()->GetPrimaryKeyReader().get();
    std::vector<primarykey_t> pks = {scan.calculatePrimaryKey("2"),
                                     scan.calculatePrimaryKey("4")};
    ASSERT_FALSE(scan.convertPK2DocId<uint64_t>(pks, {primaryKeyReaderPtr}));
}



TEST_F(SummaryScanTest, testFilterDocIdByRangeWithPkAsHashField) {
    vector<string> rawPks = {"1", "2", "123"};
    vector<primarykey_t> pks;
    SummaryScan scan;
    {
        scan._hashFuncPtr = HashFuncFactory::createHashFunc("HASH");
        buildTableInfo(scan, it_primarykey128, "id");
        scan._hashFields = {"id"};
    }
    proto::Range range;
    {
        range.set_from(0);
        range.set_to(65535);
        scan.filterDocIdByRange(rawPks, range, pks);
        vector<primarykey_t> expect = {scan.calculatePrimaryKey("1"),
                                       scan.calculatePrimaryKey("2"),
                                       scan.calculatePrimaryKey("123"),};
        ASSERT_EQ(expect.size(), pks.size());
        for (size_t i = 0; i < pks.size(); ++i) {
            EXPECT_EQ(expect[i], pks[i]);
        }
    }
    {
        rawPks.clear();
        for (size_t i = 0; i < 10000; ++i) {
            rawPks.emplace_back(autil::StringUtil::toString(i));
        }
        size_t limit = 65535;
        size_t partSize = 16384;
        for (size_t from = 0; from <= limit; from += partSize) {
            size_t to = from + partSize - 1;
            range.set_from(from);
            range.set_to(to);
            DefaultHashFunction hashFunc("HASH", 65536);
            vector<primarykey_t> expect;
            for (size_t i = 0; i < rawPks.size(); ++i) {
                auto hashId = hashFunc.getHashId(rawPks[i]);
                if (from <= hashId && hashId <= to) {
                    expect.push_back(scan.calculatePrimaryKey(rawPks[i]));
                }
            }
            scan.filterDocIdByRange(rawPks, range, pks);
            ASSERT_EQ(expect.size(), pks.size());
            for (size_t i = 0; i < pks.size(); ++i) {
                EXPECT_EQ(expect[i], pks[i]);
            }
        }
    }
}

TEST_F(SummaryScanTest, testFilterDocIdByRangeWithoutPkAsHashField) {
    vector<string> rawPks = {"1", "2", "123"};
    vector<primarykey_t> pks;
    SummaryScan scan;
    {
        scan._hashFuncPtr = HashFuncFactory::createHashFunc("HASH");
        buildTableInfo(scan, it_primarykey128, "id");
        scan._hashFields = {"attr1"};
    }
    proto::Range range;
    {
        range.set_from(0);
        range.set_to(0);
        scan.filterDocIdByRange(rawPks, range, pks);
        vector<primarykey_t> expect = {scan.calculatePrimaryKey("1"),
                                       scan.calculatePrimaryKey("2"),
                                       scan.calculatePrimaryKey("123"),};
        ASSERT_EQ(expect.size(), pks.size());
        for (size_t i = 0; i < pks.size(); ++i) {
            EXPECT_EQ(expect[i], pks[i]);
        }
    }
}

TEST_F(SummaryScanTest, testFillSummary) {
    std::vector<std::vector<std::string>> usedFieldsTestCase = {
        {"summary1", "price", "name"},
        {"summary1", "summary2", "name"},
        {"summary1"},
        {"summary2", "name"},
        {"summary1", "summary2", "price", "name"}
    };

    for (auto &usedFields : usedFieldsTestCase) {
        SummaryScan scan;
        prepareSummaryScan(scan);
        fetchAndCheckFields(scan, usedFields);
    }
}

TEST_F(SummaryScanTest, testFillSummaryFailed) {
    SummaryScan scan;
    prepareSummaryScan(scan, {10});
    scan._usedFields = {"summary1"};
    ASSERT_TRUE(scan.prepareFields());
    ASSERT_TRUE(scan.prepareSummaryReader());
    auto docs = scan._matchDocAllocator->batchAllocate(scan._docIds);
    ASSERT_FALSE(scan.fillSummary(docs));
}


TEST_F(SummaryScanTest, testFillAttributes) {
    vector<vector<string>> usedFieldsTestCase = {
        {"attr1", "attr2", "id"},
        {"attr1"},
        {"attr2"},
        {"id"},
        {"attr1", "id"}
    };
    for (auto &usedFields : usedFieldsTestCase) {
        SummaryScan scan;
        prepareSummaryScan(scan);
        fetchAndCheckFields(scan, usedFields);
    }
}

TEST_F(SummaryScanTest, testFillNotExistAttributes) {
    SummaryScan scan;
    prepareSummaryScan(scan);
    fetchAndCheckFields(scan, {"not_exist", "attr1", "not_exist1"});
}

TEST_F(SummaryScanTest, testCreateTable) {
    { //reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, false));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        SummaryScan summaryScan;
        TablePtr table = summaryScan.createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
    { //copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool, true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        SummaryScan summaryScan;
        summaryScan._pool = &_pool;
        summaryScan._memoryPoolResource = &_memPoolResource;
        TablePtr table = summaryScan.createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
}

TEST_F(SummaryScanTest, testInitSummary) {
    SummaryScan scan;
    prepareSummaryScan(scan);
    ASSERT_EQ(1, scan._indexPartitionReaderWrappers.size());
    ASSERT_EQ(1, scan._attributeExpressionCreators.size());
    ASSERT_EQ(scan._indexPartitionReaderWrapper, scan._indexPartitionReaderWrappers[0]);
    ASSERT_EQ(scan._attributeExpressionCreator, scan._attributeExpressionCreators[0]);
}

TEST_F(SummaryScanTest, testInitExtraSummary) {
    addExtraIndex();
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._tableName = "summary";
        scan.initExtraSummary(_sqlBizResource.get(), _sqlQueryResource.get());
        ASSERT_EQ(2, scan._indexPartitionReaderWrappers.size());
        ASSERT_EQ(2, scan._attributeExpressionCreators.size());
    }
    {
        SummaryScan scan;
        prepareSummaryScan(scan);
        scan._tableName = "summary2";
        scan.initExtraSummary(_sqlBizResource.get(), _sqlQueryResource.get());
        ASSERT_EQ(1, scan._indexPartitionReaderWrappers.size());
        ASSERT_EQ(1, scan._attributeExpressionCreators.size());
    }
}

TEST_F(SummaryScanTest, testMultiTableConvertPK2DocId) {
    SummaryScan scan;
    buildTableInfo(scan, it_primarykey64);
    vector<primarykey_t> pks = {scan.calculatePrimaryKey("1"),
                                scan.calculatePrimaryKey("3"),
                                scan.calculatePrimaryKey("5"),
                                scan.calculatePrimaryKey("10")};
    prepareIndexPartitionReader();
    prepareExtraIndexPartitionReader();
    scan._indexPartitionReaderWrappers =
        {_indexPartitionReaderWrapper, _extraIndexPartitionReaderWrapper};
    ASSERT_TRUE(scan.convertPK2DocId(pks));
    ASSERT_EQ(3, scan._docIds.size());
    ASSERT_EQ(0, scan._docIds[0]);
    ASSERT_EQ(2, scan._docIds[1]);
    ASSERT_EQ(0, scan._docIds[2]);
    ASSERT_EQ(3, scan._tableIdx.size());
    ASSERT_EQ(0, scan._tableIdx[0]);
    ASSERT_EQ(0, scan._tableIdx[1]);
    ASSERT_EQ(1, scan._tableIdx[2]);
}

TEST_F(SummaryScanTest, testPrepareSummaryReader) {
    SummaryScan scan;
    prepareIndexPartitionReader();
    prepareExtraIndexPartitionReader();
    scan._indexPartitionReaderWrappers =
        {_indexPartitionReaderWrapper, _extraIndexPartitionReaderWrapper};
    ASSERT_EQ(true, scan.prepareSummaryReader());
    ASSERT_EQ(2, scan._summaryReaders.size());
}

TEST_F(SummaryScanTest, testGetSummaryDocs) {
    SummaryScan scan;
    prepareSummaryScan(scan);
    prepareExtraIndexPartitionReader();
    scan._indexPartitionReaderWrappers =
        {_indexPartitionReaderWrapper, _extraIndexPartitionReaderWrapper};
    ASSERT_EQ(true, scan.prepareSummaryReader());
    std::vector<docid_t> docIds = {0, 2, 0, 2};
    scan._tableIdx = {0, 0, 1, 1};
    MatchDocAllocatorPtr matchDocAllocator(
            new MatchDocAllocator(_memPoolResource.getPool(), false));
    auto matchDocs = matchDocAllocator->batchAllocate(docIds);
    std::vector<IE_NAMESPACE(document)::SearchSummaryDocument> summaryDocs;
    ASSERT_TRUE(scan.getSummaryDocs(_sessionResource->tableInfo->getSummaryInfo(),
                                    matchDocs, summaryDocs));
    ASSERT_EQ(4, summaryDocs.size());
    ASSERT_EQ("aa", summaryDocs[0].GetFieldValue(0)->toString());
    ASSERT_EQ("cc", summaryDocs[1].GetFieldValue(0)->toString());
    ASSERT_EQ("ee", summaryDocs[2].GetFieldValue(0)->toString());
    ASSERT_EQ("gg", summaryDocs[3].GetFieldValue(0)->toString());
}

TEST_F(SummaryScanTest, testMultiTableFillAttributes) {
    addExtraIndex();
    SummaryScan scan;
    prepareSummaryScan(scan);
    scan.initExtraSummary(_sqlBizResource.get(), _sqlQueryResource.get());
    scan._usedFields = {"attr1"};
    ASSERT_TRUE(scan.prepareFields());
    scan._docIds = {0, 2, 0, 2};
    scan._tableIdx = {0, 0, 1, 1};
    auto matchDocs = scan._matchDocAllocator->batchAllocate(scan._docIds);
    ASSERT_TRUE(scan.fillAttributes(matchDocs));
    TablePtr table(new Table(matchDocs, scan._matchDocAllocator));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(table, "attr1", {0, 2, 4, 6}));
}

TEST_F(SummaryScanTest, testMultiTableDoBatchScan) {
    addExtraIndex();
    SummaryScan scan;
    prepareSummaryScan(scan);
    scan.initExtraSummary(_sqlBizResource.get(), _sqlQueryResource.get());
    scan._docIds = {0, 2, 0, 2};
    scan._tableIdx = {0, 0, 1, 1};
    scan._usedFields = {"attr1", "id", "summary1"};
    ASSERT_TRUE(scan.prepareFields());
    ASSERT_TRUE(scan.prepareSummaryReader());
    scan._calcTable.reset(new CalcTable(&_pool, &_memPoolResource,
                    {"attr1", "id", "summary1"},{}, NULL, NULL,
                    suez::turing::CavaPluginManagerPtr(), NULL));
    TablePtr table;
    bool eof;
    ASSERT_TRUE(scan.doBatchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(4, table->getRowCount());
    ASSERT_EQ(3, table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(table, "attr1", {0, 2, 4, 6}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(table, "id", {1, 3, 5, 7}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(table, "summary1",
                    {{1, 1}, {2, 1}, {3, 1}, {4, 1}}));

}

TEST_F(SummaryScanTest, testAdjustUsedField) {
    {
        SummaryScan scan;
        vector <string> expectFields = { "attr2", "attr1" };
        scan._usedFields = { "attr1", "attr2" };
        scan._outputFields = expectFields;
        scan.adjustUsedFields();
        ASSERT_EQ(expectFields, scan._usedFields);
    }
    {
        SummaryScan scan;
        vector <string> expectFields = { "attr1", "attr2" };
        scan._usedFields = expectFields;
        scan._outputFields = { "attr2", "attr3" };
        scan.adjustUsedFields();
        ASSERT_EQ(expectFields, scan._usedFields);
    }
    {
        SummaryScan scan;
        vector <string> expectFields = { "attr1", "attr2" };
        scan._usedFields = expectFields;
        scan._outputFields = { "attr1", "attr2", "attr3" };
        scan.adjustUsedFields();
        ASSERT_EQ(expectFields, scan._usedFields);
    }
}
END_HA3_NAMESPACE();
