#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/join/LookupJoinKernel.h>
#include <ha3/sql/data/TableSchema.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class LookupJoinKernelWithSubDocTest : public OpTestBase {
public:
    LookupJoinKernelWithSubDocTest();
    ~LookupJoinKernelWithSubDocTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
        _attributeMap.clear();
    }
private:
    void prepareAttributeOutSub() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$sub_id", "$path", "$group"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path", "$aid", "$sub_id", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$sub_id", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"table_name":"acl"}], "condition":{"op":"=", "params":["$sub_attr1", 1]}, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareAttributeHasSub() {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path", "$group"])json");
        _attributeMap["right_index_infos"] = ParseJson(R"json({"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}})json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path", "$aid", "$path0", "$group"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["build_node"] =
            ParseJson(R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "index_infos": {"$aid":{"type":"NUMBER", "name":"aid"}, "$path":{"type":"STRING", "name":"path"}}, "output_fields":["$aid", "$path", "$group"], "use_nest_table":true, "nest_table_attrs":[{"table_name":"acl"}], "condition":{"op":"=", "params":["$sub_attr1", 2]}, "table_type":"normal", "batch_size":1, "hash_fields":["$id"]})json");
    }

    void prepareUserIndex() override {
        prepareInvertedTable();
    }

    void prepareInvertedTable() override {
        _tableName = "acl";
        string tableName = _tableName;
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _bizInfo->_itemTableName = schemaName;
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
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

    TablePtr prepareTable() {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 1));
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {1}));
        IF_FAILURE_RET_NULL(extendMatchDocAllocator(_allocator, leftDocs, "path", {"abc"}));
        vector<string> subPaths = {"/a","/a/b/c","/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "pathM", {multiPath}));
        vector<int32_t> ids = {1, 2};
        buf = MultiValueCreator::createMultiValueBuffer(ids.data(), 2, &_pool);
        MultiInt32 multiIds;
        multiIds.init(buf);
        IF_FAILURE_RET_NULL(extendMatchDocAllocator<MultiInt32>(_allocator, leftDocs, "ids", {multiIds}));
        TablePtr table(new Table(leftDocs, _allocator));
        return table;
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
    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
            const std::string &tableName)
    {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "aid:int64;path:string;group:string;attr:int32",
                "aid:primarykey64:aid;path:string:path;attr:number:attr",
                "aid;path;group;attr",
                "aid;path;group;attr",
                "");// truncateProfile

        auto subSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "sub_id:int64;sub_attr1:int32;sub_index2:string", // fields
                "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", //indexs
                "sub_attr1;sub_id;sub_index2", //attributes
                "", // summarys
                ""); // truncateProfile

        string docsStr = "cmd=add,aid=0,path=/a,attr=1,group=groupA,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,aid=1,path=/a/b,attr=1,group=groupA,sub_id=4^5^6,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,aid=2,path=/a/b,attr=2,group=groupB,sub_id=7,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,aid=3,path=/a/b/c,attr=2,group=groupB,sub_id=8,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,aid=4,path=/a/d/c,attr=3,group=groupA,sub_id=9,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,aid=5,path=/a/c/c,attr=1,group=groupA,sub_id=10,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,aid=6,path=/a/c,attr=2,group=groupB,sub_id=11^12,sub_attr1=2^1,sub_index2=abc^ab;";

        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }


public:
    MatchDocAllocatorPtr _allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, LookupJoinKernelWithSubDocTest);


LookupJoinKernelWithSubDocTest::LookupJoinKernelWithSubDocTest() {
}

LookupJoinKernelWithSubDocTest::~LookupJoinKernelWithSubDocTest() {
}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSub) {
    prepareAttributeHasSub();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
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
    vector<string> expFid = {"0", "0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA","groupA", "groupA"};
    checkOutput(expGroup, odata, "group");
}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSubAttr) {
    prepareAttributeOutSub();
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
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
    vector<string> expFid = {"0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group");
    vector<string> subid = {"1", "4", "7"};
    checkOutput(subid, odata, "sub_id");

}

TEST_F(LookupJoinKernelWithSubDocTest, testSingleValJoinRightWithSubAttrBatchSize1) {
    prepareAttributeOutSub();
    _attributeMap["batch_size"] = 1;
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 3));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b", "/not_exist"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    {
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        ASSERT_FALSE(eof);
        vector<string> expFid = {"0"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
        vector<string> subid = {"1"};
        checkOutput(subid, odata, "sub_id");
    }
    {
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        ASSERT_FALSE(eof);
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupA"};
        checkOutput(expGroup, odata, "group");
        vector<string> subid = {"4"};
        checkOutput(subid, odata, "sub_id");
    }
    {
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        ASSERT_FALSE(eof);
        vector<string> expFid = {"1"};
        checkOutput(expFid, odata, "fid");
        vector<string> expPath = {"/a/b"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
        vector<string> subid = {"7"};
        checkOutput(subid, odata, "sub_id");
    }
    {
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        ASSERT_TRUE(eof);
    }
}


END_HA3_NAMESPACE(sql);
