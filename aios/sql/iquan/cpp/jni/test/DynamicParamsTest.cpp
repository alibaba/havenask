#include "iquan/jni/DynamicParams.h"

#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "unittest/unittest.h"

using namespace testing;
using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace iquan {

class DynamicParamsTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, DynamicParamsTest);

TEST_F(DynamicParamsTest, testGetArrayParamsOutOfRange) {
    DynamicParams dynamicParams;
    // sql index out of range
    try {
        dynamicParams.at(0);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("dynamic params idx out of range", string(e.what()));
    }
    // param index out of range
    dynamicParams._array.emplace_back(vector<Any> {});
    try {
        dynamicParams.at(0);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("dynamic params idx out of range", string(e.what()));
    }
}

TEST_F(DynamicParamsTest, testGetArrayParamsWithoutMap) {
    DynamicParams dynamicParams;
    vector<Any> dynamicParam;
    dynamicParam.push_back(Any(200));
    dynamicParam.push_back(Any(string("str1")));
    map<string, string> keyParam {{"key", "aaa"}};
    dynamicParam.push_back(Any(FastToJsonString(keyParam)));
    dynamicParams._array.push_back(dynamicParam);
    ASSERT_EQ(200, AnyCast<int>(dynamicParams.at(0)));
    ASSERT_EQ("str1", AnyCast<string>(dynamicParams.at(1)));
    ASSERT_NO_FATAL_FAILURE(autil::legacy::JsonTestUtil::checkJsonStringEqual(
        "{\n\"key\":\n  \"aaa\"\n}", AnyCast<string>(dynamicParams.at(2))));
}

TEST_F(DynamicParamsTest, testGetArrayParamsWithMap) {
    DynamicParams dynamicParams;
    vector<Any> dynamicParam;
    dynamicParam.push_back(Any(200));
    dynamicParam.push_back(Any(string("str1")));
    JsonMap keyParam {{"key", Any(string("aaa"))}};
    dynamicParam.push_back(Any(keyParam));
    dynamicParams._array.push_back(dynamicParam);
    map<string, Any> kvParam;
    kvParam["aaa"] = Any(string("bbb"));
    dynamicParams._map.push_back(kvParam);
    ASSERT_EQ(200, AnyCast<int>(dynamicParams.at(0)));
    ASSERT_EQ("str1", AnyCast<string>(dynamicParams.at(1)));
    ASSERT_EQ("bbb", AnyCast<string>(dynamicParams.at(2)));
}

TEST_F(DynamicParamsTest, testGetKVParamsFailed) {
    DynamicParams dynamicParams;
    // sql index out of range
    try {
        dynamicParams.at("xxx");
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("key [xxx] not found in dynamic params", string(e.what()));
    }
    // key not found
    dynamicParams._map.emplace_back(map<string, Any> {});
    try {
        dynamicParams.at("xxx");
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("key [xxx] not found in dynamic params", string(e.what()));
    }
}

TEST_F(DynamicParamsTest, testGetKVParamsSucceed) {
    DynamicParams dynamicParams;
    map<string, Any> kvParams;
    kvParams["xxx"] = Any(string("yyy"));
    kvParams["111"] = Any(222);
    dynamicParams._map.push_back(kvParams);
    ASSERT_EQ("yyy", AnyCast<string>(dynamicParams.at("xxx")));
    ASSERT_EQ(222, AnyCast<int>(dynamicParams.at("111")));
}

TEST_F(DynamicParamsTest, testAddKVParams) {
    DynamicParams dynamicParams;
    try {
        dynamicParams.at("xxx");
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("key [xxx] not found in dynamic params", string(e.what()));
    }
    ASSERT_FALSE(dynamicParams.addKVParams("xxx", Any(string("xxxx"))));
    try {
        dynamicParams.at("xxx");
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        ASSERT_EQ("key [xxx] not found in dynamic params", string(e.what()));
    }
    dynamicParams.reserveOneSqlParams();
    ASSERT_TRUE(dynamicParams.addKVParams("xxx", Any(string("xxxx"))));
    ASSERT_EQ("xxxx", AnyCast<string>(dynamicParams.at("xxx")));
}

TEST_F(DynamicParamsTest, testFindParamWithKey) {
    DynamicParams dynamicParams;
    ASSERT_FALSE(dynamicParams.findParamWithKey("xxx"));
    dynamicParams._map.push_back(map<string, Any> {{"aa", Any(1)}});
    ASSERT_FALSE(dynamicParams.findParamWithKey("xxx"));
    ASSERT_TRUE(dynamicParams.findParamWithKey("aa"));
}

TEST_F(DynamicParamsTest, testEmpty) {
    {
        DynamicParams dynamicParams;
        ASSERT_TRUE(dynamicParams.empty());
    }
    {
        DynamicParams dynamicParams;
        dynamicParams._array.push_back(vector<Any> {});
        ASSERT_TRUE(dynamicParams.empty());
        dynamicParams._array[0].push_back(Any(111));
        ASSERT_FALSE(dynamicParams.empty());
    }
    {
        DynamicParams dynamicParams;
        dynamicParams._map.push_back(map<string, Any> {});
        ASSERT_TRUE(dynamicParams.empty());
        dynamicParams._map[0]["xxx"] = Any(111);
        ASSERT_FALSE(dynamicParams.empty());
    }
}

TEST_F(DynamicParamsTest, testHintParam) {
    DynamicParams dynamicParams;
    ASSERT_TRUE(dynamicParams.isHintParamsEmpty());
    ASSERT_FALSE(dynamicParams.findHintParam("Xxx"));
    dynamicParams._hint["rrr"] = "yyy";
    ASSERT_TRUE(dynamicParams.findHintParam("rrr"));
    ASSERT_EQ("yyy", dynamicParams.getHintParam("rrr"));
    ASSERT_EQ("", dynamicParams.getHintParam("www"));
}

} // namespace iquan
