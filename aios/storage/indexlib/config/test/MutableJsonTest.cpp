#include "indexlib/config/MutableJson.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlibv2 { namespace config {

class MutableJsonTest : public indexlib::INDEXLIB_TESTBASE
{
public:
    MutableJsonTest() {}
    ~MutableJsonTest() {}

    DECLARE_CLASS_NAME(MutableJsonTest);

public:
    void CaseSetUp() override {}
    void CaseTearDown() override {}
    void TestSimpleProcess();
    void TestRuntimeSet();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MutableJsonTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(MutableJsonTest, TestRuntimeSet);
AUTIL_LOG_SETUP(indexlib.config, MutableJsonTest);

struct Object : public autil::legacy::Jsonizable {
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("bool_true", boolTrue, boolTrue);
    }

    bool boolTrue = false;
};

void MutableJsonTest::TestSimpleProcess()
{
    string jsonStr = R"( {
        "bool_true" : true,
        "bool_false" : false,
        "array" : [
            "string1"
        ]
    } )";
    autil::legacy::json::JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);

    MutableJson json;
    ASSERT_TRUE(json.SetValue("", jsonMap));

    // correct
    ASSERT_TRUE(json.GetAny("bool_true"));

    auto ret1 = json.GetValue<bool>("bool_true");
    ASSERT_TRUE(ret1.first.IsOK());
    ASSERT_TRUE(ret1.second);

    ASSERT_TRUE(json.GetAny("bool_false"));
    auto ret2 = json.GetValue<bool>("bool_false");
    ASSERT_TRUE(ret2.first.IsOK());
    ASSERT_FALSE(ret2.second);

    ASSERT_TRUE(json.GetAny("array"));
    vector<string> stringVector {"string1"};
    auto ret3 = json.GetValue<vector<string>>("array");
    ASSERT_TRUE(ret3.first.IsOK());
    ASSERT_EQ(stringVector, ret3.second);
    auto ret4 = json.GetValue<string>("array[0]");
    ASSERT_TRUE(ret4.first.IsOK());
    ASSERT_EQ("string1", ret4.second);

    ASSERT_TRUE(json.GetAny(""));
    Object object;
    ASSERT_FALSE(object.boolTrue);
    auto ret5 = json.GetValue<Object>("");
    ASSERT_TRUE(ret5.first.IsOK());
    ASSERT_TRUE(ret5.second.boolTrue);

    // error
    ASSERT_FALSE(json.GetAny("not_exist"));

    ASSERT_FALSE(json.GetValue<int>("bool_true").first.IsOK());
    ASSERT_FALSE(json.GetValue<int>("not_exist").first.IsOK());
    ASSERT_FALSE(json.GetValue<int>("bool_true[1]").first.IsOK());
}

void MutableJsonTest::TestRuntimeSet()
{
    MutableJson impl;
    bool value = false;

    ASSERT_FALSE(impl.GetAny("bool_true"));

    ASSERT_TRUE(impl.SetValue("bool_true", true));
    ASSERT_TRUE(impl.SetValue("array[1]", "string1"));
    ASSERT_TRUE(impl.SetValue("map0.testvalue", 100));
    ASSERT_TRUE(impl.SetValue("map0.testvalue", 200));

    value = impl.GetValue<bool>("bool_true").second;
    ASSERT_TRUE(value);
    std::string strValue = impl.GetValue<std::string>("array[1]").second;
    ASSERT_EQ("string1", strValue);
    ASSERT_FALSE(impl.GetValue<std::string>("array[0]").first.IsOK());
    int intVal = impl.GetValue<int>("map0.testvalue").second;
    ASSERT_EQ(200, intVal);

    ASSERT_TRUE(impl.SetValue("map0.testvalue.hello", "world"));
    ASSERT_EQ("world", impl.GetValue<std::string>("map0.testvalue.hello").second);

    ASSERT_TRUE(impl.SetValue("map0.testvalue.hello[3]", "world"));
    ASSERT_EQ("world", impl.GetValue<std::string>("map0.testvalue.hello[3]").second);

    ASSERT_TRUE(impl.SetValue("map0.testvalue.hello[4]", "world1"));
    ASSERT_EQ("world1", impl.GetValue<std::string>("map0.testvalue.hello[4]").second);

    ASSERT_TRUE(impl.SetValue("map0.testvalue.hello[2].testvalue", 300));
    ASSERT_EQ(300, impl.GetValue<int>("map0.testvalue.hello[2].testvalue").second);

    ASSERT_EQ("world", impl.GetValue<std::string>("map0.testvalue.hello[3]").second);

    ASSERT_TRUE(impl.SetValue("map0.testvalue", 1));
    ASSERT_FALSE(impl.GetValue<std::string>("map0.testvalue.hello[3]").first.IsOK());
    ASSERT_EQ(1, impl.GetValue<int>("map0.testvalue").second);

    ASSERT_TRUE(impl.SetValue("map0.testvalue.hello[3][2]", 123));
    ASSERT_FALSE(impl.GetValue<std::string>("map0.testvalue.hello[3]").first.IsOK());
    ASSERT_EQ(123, impl.GetValue<int>("map0.testvalue.hello[3][2]").second);

    struct TestStruct : public autil::legacy::Jsonizable {
        int a = 0;
        int b = 0;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) final override
        {
            json.Jsonize("a", a, a);
            json.Jsonize("b", b, b);
        }
    };
    TestStruct struct0;
    struct0.a = 1;
    struct0.b = 2;
    ASSERT_TRUE(impl.SetValue("map0.testvalue.x[3].testStruct", struct0));
    TestStruct struct1;
    ASSERT_TRUE(impl.GetValue<TestStruct>("map0.testvalue.x[3].testStruct").first.IsOK());
    ASSERT_EQ(struct0.a, impl.GetValue<TestStruct>("map0.testvalue.x[3].testStruct").second.a);
    ASSERT_EQ(struct0.b, impl.GetValue<TestStruct>("map0.testvalue.x[3].testStruct").second.b);

    ASSERT_EQ(1, impl.GetValue<int>("map0.testvalue.x[3].testStruct.a").second);
}

}} // namespace indexlibv2::config
