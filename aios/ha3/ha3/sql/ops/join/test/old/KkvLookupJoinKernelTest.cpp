#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/join/KkvLookupJoinKernel.h>
#include <ha3/sql/data/TableSchema.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class KkvLookupJoinKernelTest : public OpTestBase {
public:
    KkvLookupJoinKernelTest();
    ~KkvLookupJoinKernelTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needKkvTable = true;
        _needExprResource = true;
        prepareAttribute();
    }
    void tearDown() override {
        _attributeMap.clear();
    }
private:
    void prepareAttribute() {
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$fid", "$path"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$path", "$group"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$fid", "$path", "$path0", "$group0"])json");
        _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$path", "$path0"]})json");
        _attributeMap["kkv_table_name"] = string("acl");
    }

    void prepareKkvTableData(string &tableName, string &fields,
                             string &pkeyField, string &skeyField,
                             string &attributes, string &docs, int64_t &ttl) override
    {
        _tableName = "acl";
        tableName = _tableName;
        fields = "aid:int64;path:string;group:string";
        pkeyField = "path";
        skeyField = "aid";
        attributes = "aid;path;group";
        docs = "cmd=add,aid=0,path=/a,group=groupA;"
               "cmd=add,aid=1,path=/a/b,group=groupA;"
               "cmd=add,aid=2,path=/a/b,group=groupB;"
               "cmd=add,aid=3,path=/a/b/c,group=groupB;";
        ttl = numeric_limits<int64_t>::max();
    }

    void setResource(navi::KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    navi::KernelTesterPtr buildTester(navi::KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("KkvLookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return navi::KernelTesterPtr(builder.build());
    }

    void checkOutput(const vector<string> &expect, navi::DataPtr data, const string &field) {
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
};

KkvLookupJoinKernelTest::KkvLookupJoinKernelTest() {
}

KkvLookupJoinKernelTest::~KkvLookupJoinKernelTest() {
}

TEST_F(KkvLookupJoinKernelTest, testSimpleProcess) {
    auto testerPtr = buildTester(navi::KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, leftDocs, "path", {"/a", "/a/b"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(navi::EC_NONE, testerPtr->getErrorCode());

    navi::DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "1", "1"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path");
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group0");
}

TEST_F(KkvLookupJoinKernelTest, testMultiJoin) {
    auto testerPtr = buildTester(navi::KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = move(getMatchDocs(_allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "fid", {0}));
        vector<string> subPaths = {"/", "/a", "/a/b"};
        char* buf = MultiValueCreator::createMultiStringBuffer(subPaths, &_pool);
        MultiString multiPath;
        multiPath.init(buf);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiString>(_allocator, leftDocs, "path", {multiPath}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable));
        ASSERT_TRUE(testerPtr->setInputEof("input0"));
    }
    ASSERT_TRUE(testerPtr->compute()); // kernel compute success
    ASSERT_EQ(navi::EC_NONE, testerPtr->getErrorCode());

    navi::DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    vector<string> expFid = {"0", "0", "0"};
    checkOutput(expFid, odata, "fid");
    vector<string> expPath = {"/a", "/a/b", "/a/b"};
    checkOutput(expPath, odata, "path0");
    vector<string> expGroup = {"groupA", "groupA", "groupB"};
    checkOutput(expGroup, odata, "group0");
}

END_HA3_NAMESPACE(sql);
