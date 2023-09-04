#include "indexlib/util/KeyValueMap.h"

#include "autil/legacy/legacy_jsonizable.h"
#include "unittest/unittest.h"

namespace indexlib::util {

class KeyValueMapTest : public TESTBASE
{
public:
    KeyValueMapTest() = default;
    ~KeyValueMapTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void KeyValueMapTest::setUp() {}

void KeyValueMapTest::tearDown() {}

TEST_F(KeyValueMapTest, testGetTypeValueFromJsonMap)
{
    std::string jsonStr = R"({
    "string_key" : "string_value",
    "list_key": [1, 2],
    "map_key": { "key1": "value1" }
    })";

    autil::legacy::json::JsonMap jsonMap;
    autil::legacy::FromJsonString(jsonMap, jsonStr);

    ASSERT_EQ("string_value", GetTypeValueFromJsonMap<std::string>(jsonMap, "string_key", "").value());
    ASSERT_EQ("", GetTypeValueFromJsonMap<std::string>(jsonMap, "non_key", "").value());
    auto stringIt = jsonMap.find("string_key");
    ASSERT_TRUE(stringIt != jsonMap.end());
    ASSERT_EQ("string_value", autil::legacy::AnyCast<std::string>(stringIt->second));
    ASSERT_EQ(jsonMap.end(), jsonMap.find("non_key"));

    std::vector<int32_t> expectVector {1, 2};
    ASSERT_EQ(expectVector, GetTypeValueFromJsonMap(jsonMap, "list_key", std::vector<int32_t>()).value());
    ASSERT_EQ(expectVector, *GetTypeValueFromJsonMap(jsonMap, "list_key", std::vector<int32_t>()));
    ASSERT_FALSE(GetTypeValueFromJsonMap(jsonMap, "list_key", std::string("default")).has_value());
    ASSERT_FALSE(GetTypeValueFromJsonMap(jsonMap, "list_key", std::string("default")));

    std::map<std::string, std::string> expectMap {{"key1", "value1"}};
    ASSERT_EQ(expectMap, GetTypeValueFromJsonMap(jsonMap, "map_key", std::map<std::string, std::string>()));

    auto mapIt = jsonMap.find("map_key");
    ASSERT_TRUE(mapIt != jsonMap.end());
    ASSERT_ANY_THROW(autil::legacy::AnyCast<std::string>(mapIt->second));
    ASSERT_FALSE(autil::legacy::AnyCast<std::string>(&mapIt->second));
}

} // namespace indexlib::util
