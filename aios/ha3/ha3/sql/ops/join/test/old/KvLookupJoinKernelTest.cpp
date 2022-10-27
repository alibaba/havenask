#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/join/KvLookupJoinKernel.h>
#include <ha3/sql/data/TableSchema.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class KvLookupJoinKernelTest : public OpTestBase {
public:
    KvLookupJoinKernelTest();
    ~KvLookupJoinKernelTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needKvTable = true;
        _needExprResource = true;
        prepareAttribute();
    }
    void tearDown() override {
        _attributeMap.clear();
    }
private:
    void prepareAttribute() {
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] =
            ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name"])json");
        _attributeMap["condition"] =
            ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid", "$cid0"]})json");
        _attributeMap["kv_table_name"] = string("y");
        _attributeMap["batch_size"] = Any(100);
    }

    void prepareKvTableData(string &tableName, string &fields,
                            string &pkeyField, string &attributes,
                            string &docs, bool &enableTTL, int64_t &ttl) override
    {
        _tableName = "y";
        tableName = _tableName;
        fields = "cid:uint32;group_name:string";
        pkeyField = "cid";
        attributes = "cid;group_name";
        docs = "cmd=add,cid=0,group_name=groupA;"
               "cmd=add,cid=1,group_name=groupB;"
               "cmd=add,cid=2,group_name=groupC;";
        enableTTL = false;
        ttl = numeric_limits<int64_t>::max();
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("KvLookupJoinKernel");
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
};

KvLookupJoinKernelTest::KvLookupJoinKernelTest() {
}

KvLookupJoinKernelTest::~KvLookupJoinKernelTest() {
}

TEST_F(KvLookupJoinKernelTest, testSimpleProcess) {
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "cid", {0, 1}));
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
    vector<string> expXUid = {"0", "0"};
    checkOutput(expXUid, odata, "uid");
    vector<string> expXCid = {"0", "1"};
    checkOutput(expXCid, odata, "cid");
    vector<string> expYCid = {"0", "1"};
    checkOutput(expYCid, odata, "cid0");
    vector<string> expYName = {"groupA", "groupB"};
    checkOutput(expYName, odata, "group_name");
}

TEST_F(KvLookupJoinKernelTest, testMultiJoin) {
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 1));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {0}));
        int32_t dataArray[] = {0, 2};
        char *buf = MultiValueCreator::createMultiValueBuffer(dataArray, 2, &_pool);
        MultiValueType<uint32_t> multiValue(buf);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiValueType<uint32_t>>(
                        _allocator, leftDocs, "cid", {multiValue}));
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
    vector<string> expXUid = {"0", "0"};
    checkOutput(expXUid, odata, "uid");
    vector<string> expYCid = {"0", "2"};
    checkOutput(expYCid, odata, "cid0");
    vector<string> expYName = {"groupA", "groupC"};
    checkOutput(expYName, odata, "group_name");
}

END_HA3_NAMESPACE(sql);
