#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/data/TableSchema.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class LookupJoinKernelWithSummaryTest : public OpTestBase {
public:
    LookupJoinKernelWithSummaryTest();
    ~LookupJoinKernelWithSummaryTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        _equiCondition = "{\"op\":\"=\", \"type\":\"OTHER\", \"params\":[\"$aid\", \"$aid0\"]}";
        _partialEquiCondition = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$aid\",\"$aid0\"]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$path\",\"$path0\"]}]}";
        _buildNodeWithCompareCondition = "{\"table_name\":\"acl\", \"db_name\":\"default\", \"catalog_name\":\"default\", \"index_infos\": {\"$aid\":{\"type\":\"NUMBER\", \"name\":\"aid\"}, \"$path\":{\"type\":\"STRING\", \"name\":\"path\"}, \"$attr\":{\"type\":\"NUMBER\", \"name\":\"attr\"}}, \"output_fields\":[\"$aid\", \"$path\", \"$group\"], \"use_nest_table\":false, \"table_type\":\"summary\", \"batch_size\":1, \"hash_type\":\"HASH\", \"used_fields\":[\"$aid\", \"$path\", \"$group\"], \"hash_fields\":[\"$aid\"], \"condition\": {\"op\":\">\",\"type\":\"OTHER\",\"params\":[\"$aid\",3]}}";
    }
    void tearDown() override {
        _attributeMap.clear();
    }
private:
    string _equiCondition;
    string _partialEquiCondition;
    string _buildNodeWithCompareCondition;
private:
    // summary table fields: aid, path, group
    // input table fields: aid, path
    void prepareAttribute(bool leftIsBuild, string joinType, string equiCondition, string buildNode = "") {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = leftIsBuild;
        _attributeMap["join_type"] = joinType;
        if (joinType == "SEMI" || joinType == "ANTI") {
            _attributeMap["semi_join_type"] = joinType;
        }
        if (leftIsBuild) {
            _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
            _attributeMap["left_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
            _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        } else {
            _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
            _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
            _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}, "$attr":{"type":"NUMBER", "name":"attr"}})json");
        }
        _attributeMap["system_field_num"] = Any(0);
        if (joinType == "SEMI" || joinType == "ANTI") {
            if (leftIsBuild) {
                _attributeMap["output_fields"] =
                    ParseJson(R"json(["$aid", "$path", "$group"])json");
                _attributeMap["output_fields_internal"] =
                    ParseJson(R"json(["$aid", "$path", "$group", "$aid0", "$path0"])json");
            } else {
                _attributeMap["output_fields"] =
                    ParseJson(R"json(["$aid", "$path"])json");
                _attributeMap["output_fields_internal"] =
                    ParseJson(R"json(["$aid", "$path", "$aid0", "$path0", "$group0"])json");
            }
        } else {
            if (leftIsBuild) {
                _attributeMap["output_fields"] =
                    ParseJson(R"json(["$aid", "$path", "$group", "$aid0", "$path0"])json");
            } else {
                _attributeMap["output_fields"] =
                    ParseJson(R"json(["$aid", "$path", "$aid0", "$path0", "$group"])json");
            }
        }
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(equiCondition);
        if (buildNode.empty()) {
            _attributeMap["build_node"] =
                ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}, "$attr":{"type":"NUMBER", "name":"attr"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"summary", "batch_size":1, "hash_type":"HASH", "used_fields":["$aid", "$path", "$group"], "hash_fields":["$aid"]})json");
        } else {
            _attributeMap["build_node"] = ParseJson(buildNode);
        }
    }

    void prepareInvertedTableData(string &tableName,
                                  string &fields, string &indexes, string &attributes,
                                  string &summarys, string &truncateProfileStr,
                                  string &docs, int64_t &ttl) override
    {
        _tableName = "acl";
        tableName = _tableName;
        fields = "aid:int64;path:string;group:string;attr:int32";
        indexes = "aid:primarykey64:aid;path:string:path;attr:number:attr";
        attributes = "aid;path;group;attr";
        summarys = "aid;path;group;attr";
        truncateProfileStr = "";
        docs = "cmd=add,aid=0,path=/a,attr=1,group=groupA;"
               "cmd=add,aid=1,path=/a/b,attr=1,group=groupA;"
               "cmd=add,aid=2,path=/a/b,attr=2,group=groupB;"
               "cmd=add,aid=3,path=/a/b/c,attr=2,group=groupB;"
               "cmd=add,aid=4,path=/a/d/c,attr=3,group=groupA;"
               "cmd=add,aid=5,path=/a/c/c,attr=1,group=groupA;"
               "cmd=add,aid=6,path=/a/c,attr=2,group=groupB;";
        ttl = numeric_limits<int64_t>::max();
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        _sqlResource->range.set_from(0);
        _sqlResource->range.set_to(65535);
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount);
        ColumnPtr column = outputTable->getColumn(field);
        auto name = column->getColumnSchema()->getName();
        ASSERT_TRUE(column != NULL);
        for (size_t i = 0; i < rowCount; i++) {
            //cout << name << " : " << column->toString(i) << endl;
            ASSERT_EQ(expect[i], column->toString(i));
        }
    }

public:
    MatchDocAllocatorPtr _allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, LookupJoinKernelWithSummaryTest);


LookupJoinKernelWithSummaryTest::LookupJoinKernelWithSummaryTest() {
}

LookupJoinKernelWithSummaryTest::~LookupJoinKernelWithSummaryTest() {
}

TEST_F(LookupJoinKernelWithSummaryTest, testErrorKernel) {
    {
        prepareAttribute(true, "LEFT", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    {
        prepareAttribute(true, "ANTI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValLeftJoinRight) {
    {
        prepareAttribute(false, "LEFT", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "3", "6", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"0", "3", "6", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a", "/a/b", "/a/c", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/c", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB", "groupB", ""};
        checkOutput(expGroup, odata, "group");        
    }
    {
        prepareAttribute(false, "LEFT", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6", "0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"6", "0", "0", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a/c", "/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a/c", "", "", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupB", "", "", ""};
        checkOutput(expGroup, odata, "group");        
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValAntiJoinRight) {
    {
        prepareAttribute(false, "ANTI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/b/c"};
        checkOutput(expPath, odata, "path");
    }
    {
        prepareAttribute(false, "ANTI", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValInnerJoinLeft) {
    {
        prepareAttribute(true, "INNER", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "3", "6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a", "/a/b/c", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/b", "/a/c"};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB", "groupB"};
        checkOutput(expGroup, odata, "group");        
    }
    {
        prepareAttribute(true, "INNER", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");        
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValSemiJoinLeft) {
    {
        prepareAttribute(true, "SEMI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "3", "6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b/c", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupA", "groupB", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "SEMI", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexLeftJoinRight) {
    {
        prepareAttribute(false, "LEFT", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "6", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"0", "6", "0", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a", "/a/c", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/c", "", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB", "", ""};
        checkOutput(expGroup, odata, "group");        
    }
    {
        prepareAttribute(false, "LEFT", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6", "0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"6", "0", "0", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a/c", "/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a/c", "", "", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupB", "", "", ""};
        checkOutput(expGroup, odata, "group");        
    }
}


TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexAntiJoinRight) {
    {
        prepareAttribute(false, "ANTI", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
    {
        prepareAttribute(false, "ANTI", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexInnerJoinRight) {
    {
        prepareAttribute(true, "INNER", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/c"};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB"};
        checkOutput(expGroup, odata, "group");
    }    
    {
        prepareAttribute(true, "INNER", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }    
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexSemiJoinLeft) {
    {
        prepareAttribute(true, "SEMI", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"0", "6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupA", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "SEMI", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
            ASSERT_TRUE(testerPtr->setInputEof("input0"));
        }
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }    
}


END_HA3_NAMESPACE(sql);
