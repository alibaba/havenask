#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <autil/StringUtil.h>
#include <autil/HashFuncFactory.h>

using namespace std;
using namespace testing;
using namespace autil;
using namespace navi;
using namespace suez::turing;
using namespace suez;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil_rapidjson;

BEGIN_HA3_NAMESPACE(sql);

class SummaryTestTable {
public:
    string buildDocs() {
        string docs;
        for (size_t i = 0; i < docCount; ++i) {
            docs += "cmd=add";
            for (auto &pair : data) {
                docs += "," + pair.first + "=" + getData(pair.first, i);
            }
            docs += ";";
        }
        return docs;
    }
    string getData(const string &key, size_t idx) {
        if (data.find(key) == data.end()) {
            return "null";
        }
        std::string str = StringUtil::split(data[key], ",")[idx];
        StringUtil::trim(str);
        StringUtil::replaceAll(str, "|", " ");
        return str;
    }
public:
    string fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;"
                    "price:double;summary1:int32:true;summary2:string:true;";
    string indexes = "pk:primarykey64:id";
    string attributes = "attr1;attr2;id";
    string summarys = "name;price;summary1;summary2;id";
    size_t docCount = 4;
    map<string, string> data = {
        { "attr1", "0, 0, 0, 0" },
        { "attr2", "0|10, 1|11, 2|22, 3|33" },
        { "id", "1, 3, 5, 7" },
        { "name", "aa, bb, cc, dd" },
        { "price", "1.1, 2.2, 3.3, 4.4" },
        { "summary1", "1|1, 1|2, 2|1, 2|2" },
        { "summary2", "a|a|a, a|b|c, a|c|stop, b|c|d" }
    };
};

class SummaryScanKernelTest : public OpTestBase {
public:
    SummaryScanKernelTest() {
        _tableName = "summary";
    }
    void setUp();
    void tearDown();
    void prepareUserIndex() override {
        prepareSummaryTable();
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
    }

    void prepareSummaryPK64TableData(string &tableName,
            string &fields, string &indexes, string &attributes,
            string &summarys, string &truncateProfileStr,
            string &docs, int64_t &ttl)
    {
        tableName = "summary";
        fields = _summaryTestTable.fields;
        indexes = _summaryTestTable.indexes;
        attributes = _summaryTestTable.attributes;
        summarys = _summaryTestTable.summarys;
        truncateProfileStr = "";
        docs = _summaryTestTable.buildDocs();
        ttl = numeric_limits<int64_t>::max();
    }

    void prepareExtraSummaryPK64TableData(string &tableName,
            string &fields, string &indexes, string &attributes,
            string &summarys, string &truncateProfileStr,
            string &docs, int64_t &ttl)
    {
        _extraSummaryTestTable.data = {
            { "attr1", "0, 0, 0, 0" },
            { "attr2", "0|10, 1|11, 2|22, 3|33" },
            { "id", "2, 4, 6, 8" },
            { "name", "ee, ff, gg, hh" },
            { "price", "1.1, 2.2, 3.3, 4.4" },
            { "summary1", "1|1, 1|2, 2|1, 2|2" },
            { "summary2", "a|a|a, a|b|c, a|c|stop, b|c|d" }
        };
        tableName = "summary_extra";
        fields = _extraSummaryTestTable.fields;
        indexes = _extraSummaryTestTable.indexes;
        attributes = _extraSummaryTestTable.attributes;
        summarys = _extraSummaryTestTable.summarys;
        truncateProfileStr = "";
        docs = _extraSummaryTestTable.buildDocs();
        ttl = numeric_limits<int64_t>::max();
    }

    void prepareAttributeMap() {
        _attributeMap["table_type"] = string("summary");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["hash_type"] = string("HASH");
        _attributeMap["hash_fields"] = ParseJson(R"json(["id"])json");
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        _sqlResource->range.set_from(0);
        _sqlResource->range.set_to(65535);
        _sessionResource->dependencyTableInfoMap[_tableName + "_extra"] =
            suez::turing::TableInfoConfigurator::createFromIndexApp(_bizInfo->_itemTableName, _indexApp);
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("ScanKernel");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }

    void getOutputTable(KernelTester &tester, TablePtr &outputTable) {
        DataPtr outputData;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        outputTable = getTable(outputData);
        ASSERT_TRUE(outputTable != nullptr);
    }

    void runKernel(TablePtr &outputTable) {
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        auto &tester = *testerPtr;
        ASSERT_TRUE(tester.compute()); // kernel compute success
        getOutputTable(tester, outputTable);
    }

    void runKernelFailed(TablePtr &outputTable) {
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }

    void checkOutput(const TablePtr &table,
                     const vector<uint32_t> &rawDocIdx,
                     const std::vector<uint32_t> &tableIdx,
                     const vector<std::string> &columns)
    {
        ASSERT_EQ(rawDocIdx.size(), table->getRowCount());
        ASSERT_EQ(columns.size(), table->getColumnCount());
        for (size_t i = 0; i < rawDocIdx.size(); ++i) {
            for (auto &column : columns) {
                string desc = "docId: " + StringUtil::toString(rawDocIdx[i]) +
                              ", row: " + StringUtil::toString(rawDocIdx[i]) +
                              ", column: " + column;
                string expect = tableIdx[i] == 0 ?
                                _summaryTestTable.getData(column, rawDocIdx[i])
                                : _extraSummaryTestTable.getData(column, rawDocIdx[i]);
                StringUtil::replaceAll(expect, " ", "^]");
                string actual = table->getColumn(column)->toString(i);
                ASSERT_EQ(expect, actual) << desc;
            }
        }
    }

    void checkNormalCase(const std::string &conditionJson,
                         const std::string &outputFieldsJson,
                         const std::string &usedFieldsJson,
                         const std::vector<uint32_t> &expectedRawDocIdx,
                         const std::vector<uint32_t> &expectedTableIdx,
                         const std::vector<std::string> &checkColumns,
                         size_t limit = 1000,
                         bool willRunKernelFailed = false)
    {
        prepareAttributeMap();
        _attributeMap["condition"] = ParseJson(conditionJson);
        _attributeMap["output_fields"] = ParseJson(outputFieldsJson);
        _attributeMap["used_fields"] = ParseJson(usedFieldsJson);
        _attributeMap["limit"] = Any(limit);
        TablePtr table;
        if (willRunKernelFailed) {
            ASSERT_NO_FATAL_FAILURE(runKernelFailed(table));
        } else {
            ASSERT_NO_FATAL_FAILURE(runKernel(table));
            ASSERT_NO_FATAL_FAILURE(
                    checkOutput(table, expectedRawDocIdx, expectedTableIdx, checkColumns));
        }
    }

private:
    SummaryTestTable _summaryTestTable;
    SummaryTestTable _extraSummaryTestTable;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, SummaryScanKernelTest);

void SummaryScanKernelTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = true;
}

void SummaryScanKernelTest::tearDown() {
}

TEST_F(SummaryScanKernelTest, testQueryWithoutId) {
    checkNormalCase(
            R"json({})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {}, {}, {"id", "price", "summary1"}, 1000, true);
}

TEST_F(SummaryScanKernelTest, testSample) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 3, 5, 7]})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {1, 2, 3}, {0, 0, 0}, {"id", "price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testNotExist) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 3, 5, 7]})json",
            R"json(["$id", "$price", "$summary1", "not_exist"])json",
            R"json(["$id", "$price", "$summary1", "not_exist"])json",
            {1, 2, 3}, {0, 0, 0}, {"id", "price", "summary1", "not_exist"});
}

TEST_F(SummaryScanKernelTest, testHaIn) {
    checkNormalCase(
            R"json({"op": "ha_in", "params": ["$id", "3|5|7"],"type":"UDF"})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {1, 2, 3}, {0, 0, 0}, {"id", "price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testExceedId) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 0, 3, 15, 5, 16, 7]})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {1, 2, 3}, {0, 0, 0}, {"id", "price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testLimit) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 1, 3, 5, 7]})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {0, 1}, {0, 0}, {"id", "price", "summary1"}, 2);
}

TEST_F(SummaryScanKernelTest, testProject) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 1, 3, 5, 7]})json",
            R"json(["$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {0, 1, 2, 3}, {0, 0, 0, 0}, {"price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testParallel) {
    size_t parallelNum = 2;
    size_t parallelIndex = 0;
    _attributeMap["parallel_num"] = Any(parallelNum);
    _attributeMap["parallel_index"] = Any(parallelIndex);
    vector<string> rawPks = {"1", "3", "5", "7"};
    auto hashFuncPtr = HashFuncFactory::createHashFunc("HASH");
    vector<uint32_t> expects;
    vector<uint32_t> expectsIdx;
    for (size_t i = 0; i < rawPks.size(); ++i) {
        if (hashFuncPtr->getHashId(rawPks[i]) % parallelNum == parallelIndex) {
            expects.emplace_back(i);
            expectsIdx.emplace_back(0);
        }
    }
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 1, 3, 5, 7]})json",
            R"json(["$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            expects, expectsIdx, {"price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testCondition) {
    checkNormalCase(
            R"json({"op": "AND", "params": [
                   {"op": "IN", "params": ["$id", 1, 3, 5, 7]},
                   {"op": ">", "params": ["$price", 2]}]})json",
            R"json(["$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {1, 2, 3}, {0, 0, 0}, {"price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testConditionHaIn) {
    checkNormalCase(
            R"json({"op": "AND", "params": [
                   {"op": "ha_in", "params": ["$id", "1|3|5|7"], "type":"UDF"},
                   {"op": ">", "params": ["$price", 2]}]})json",
            R"json(["$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {1, 2, 3}, {0, 0, 0}, {"price", "summary1"});
}

TEST_F(SummaryScanKernelTest, testExtraSummary) {
    checkNormalCase(
            R"json({"op": "IN", "params": ["$id", 2, 3, 4, 7, 6, 10]})json",
            R"json(["$id", "$price", "$summary1"])json",
            R"json(["$id", "$price", "$summary1"])json",
            {0, 1, 1, 3, 2}, {1, 0, 1, 0, 1}, {"id", "price", "summary1"});
}

END_HA3_NAMESPACE();
