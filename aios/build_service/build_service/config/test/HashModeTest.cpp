#include "build_service/test/unittest.h"
#include "build_service/config/HashMode.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace config {

class HashModeTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void HashModeTest::setUp() {
}

void HashModeTest::tearDown() {
}

TEST_F(HashModeTest, testHashMode) {
    { // empty hash field
        string hashModeStr = R"({
        "hash_field":"",
        "hash_function":"fun"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // empty hash field
        string hashModeStr = R"({
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // empty hash name
        string hashModeStr = R"({
        "hash_field":"aa",
        "hash_function":""
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_FALSE(hashMode.validate());
    }
    { // normal
        string hashModeStr = R"({
        "hash_field":"abc"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("HASH", hashMode._hashFunction);
        EXPECT_EQ("abc", hashMode._hashField);
        EXPECT_EQ(1, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
    }

    { // normal
        string hashModeStr = R"({
        "hash_field":"abc",
        "hash_function":"fun"
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("fun", hashMode._hashFunction);
        EXPECT_EQ("abc", hashMode._hashField);
        EXPECT_EQ(1, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
    }
    { // normal with multi hash fields
        string hashModeStr = R"({
        "hash_fields":["abc", "ef"],
        "hash_function":"fun",
        "hash_params": {"a":"b"}
})";
        HashMode hashMode;
        EXPECT_NO_THROW(FromJsonString(hashMode, hashModeStr));
        EXPECT_TRUE(hashMode.validate());
        EXPECT_EQ("fun", hashMode._hashFunction);
        EXPECT_EQ("", hashMode._hashField);
        EXPECT_EQ(2, hashMode._hashFields.size());
        EXPECT_EQ("abc", hashMode._hashFields[0]);
        EXPECT_EQ("ef", hashMode._hashFields[1]);
        EXPECT_EQ("b", hashMode._hashParams["a"]);
    }

}
TEST_F(HashModeTest, testRegionHashMode) {
    { // empty hash field
        string regionHashModeStr = R"({
        "hash_field":"",
        "hash_function":"fun",
        "region_name":"aaa"
})";
        RegionHashMode regionHashMode;
        EXPECT_NO_THROW(FromJsonString(regionHashMode, regionHashModeStr));
        EXPECT_FALSE(regionHashMode.validate());
    }
    { // empty hash field
        string regionHashModeStr = R"({
})";
        RegionHashMode regionHashMode;
        EXPECT_NO_THROW(FromJsonString(regionHashMode, regionHashModeStr));
        EXPECT_FALSE(regionHashMode.validate());
    }
    { // empty ragion name
        string regionHashModeStr = R"({
        "hash_field":"abc",
        "hash_function":"fun",
        "region_name":""
})";
        RegionHashMode regionHashMode;
        EXPECT_NO_THROW(FromJsonString(regionHashMode, regionHashModeStr));
        EXPECT_FALSE(regionHashMode.validate());
    }
    { // normal
        string regionHashModeStr = R"({
        "hash_field":"abc",
        "hash_function":"fun",
        "region_name":"aaa"
})";
        RegionHashMode regionHashMode;
        EXPECT_NO_THROW(FromJsonString(regionHashMode, regionHashModeStr));
        EXPECT_TRUE(regionHashMode.validate());
        EXPECT_EQ("fun", regionHashMode._hashFunction);
        EXPECT_EQ("abc", regionHashMode._hashField);
        EXPECT_EQ(1, regionHashMode._hashFields.size());
        EXPECT_EQ("abc", regionHashMode._hashFields[0]);
        EXPECT_EQ("aaa", regionHashMode._regionName);
    }
    { // normal with multi hash fields
        string regionHashModeStr = R"({
        "hash_fields":["abc", "ef"],
        "hash_function":"fun",
        "hash_params": {"a":"b"},
        "region_name":"aaa"
})";
        RegionHashMode regionHashMode;
        EXPECT_NO_THROW(FromJsonString(regionHashMode, regionHashModeStr));
        EXPECT_TRUE(regionHashMode.validate());
        EXPECT_EQ("fun", regionHashMode._hashFunction);
        EXPECT_EQ("", regionHashMode._hashField);
        EXPECT_EQ(2, regionHashMode._hashFields.size());
        EXPECT_EQ("abc", regionHashMode._hashFields[0]);
        EXPECT_EQ("ef", regionHashMode._hashFields[1]);
        EXPECT_EQ("b", regionHashMode._hashParams["a"]);
        EXPECT_EQ("aaa", regionHashMode._regionName);
    }

}
}
}
