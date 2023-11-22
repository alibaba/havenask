#include "indexlib/util/JsonUtil.h"

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace util {

class JsonUtilTest : public TESTBASE
{
public:
    JsonUtilTest() {}
    ~JsonUtilTest() {}

public:
    void setUp() override {}
    void tearDown() override {}

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.util, JsonUtilTest);

TEST_F(JsonUtilTest, TestCast)
{
    string jsonMapStr = R"({
    "string_key": "string_value",
    "list_key": [1, 2],
    "map_key": {
        "key1": "value1",
        "key2": 2
    }
    })";

    autil::legacy::Any jsonMapAny = *JsonUtil::JsonStringToAny(jsonMapStr);
    ASSERT_TRUE(JsonUtil::AnyToJsonMap(jsonMapAny));
    ASSERT_FALSE(JsonUtil::AnyToJsonArray(jsonMapAny));
    ASSERT_FALSE(JsonUtil::AnyToString(jsonMapAny));

    string jsonArrayStr = R"(["string", 1, {}])";
    autil::legacy::Any jsonArrayAny = *JsonUtil::JsonStringToAny(jsonArrayStr);
    ASSERT_FALSE(JsonUtil::AnyToJsonMap(jsonArrayAny));
    ASSERT_TRUE(JsonUtil::AnyToJsonArray(jsonArrayAny));
    ASSERT_FALSE(JsonUtil::AnyToString(jsonArrayAny));

    string stringStr = R"("string")";
    autil::legacy::Any jsonStringAny = *JsonUtil::JsonStringToAny(stringStr);
    ASSERT_FALSE(JsonUtil::AnyToJsonMap(jsonStringAny));
    ASSERT_FALSE(JsonUtil::AnyToJsonArray(jsonStringAny));
    ASSERT_EQ("string", *JsonUtil::AnyToString(jsonStringAny));

    ASSERT_EQ("string", *JsonUtil::AnyToString(JsonUtil::StringToAny("string")));
}

}} // namespace indexlib::util
