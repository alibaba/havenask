/**
 * @file   knn_util_test.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Tue Jan 22 11:01:32 2019
 *
 * @brief
 *
 *
 */
#include "aitheta_indexer/plugins/aitheta/util/param_util.h"

#include <string>
#include <map>
#include <unittest/unittest.h>
#include "aitheta_indexer/test/test.h"
#include "aitheta_indexer/plugins/aitheta/common_define.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(aitheta_plugin);

class ParamUtilTest : public ::testing::Test {
public:
    ParamUtilTest() {
    }

    virtual ~ParamUtilTest() {
    }

    virtual void SetUp() {
    }

    virtual void TearDown() {
    }

    void checkKeyValueMap(const KeyValueMap &expectParams, const KeyValueMap &params) {
        for (const auto kv : expectParams) {
            const auto itr = params.find(kv.first);
            ASSERT_TRUE(itr != params.end());
            ASSERT_EQ(kv.second, itr->second);
        }
        for (const auto kv : params) {
            const auto itr = expectParams.find(kv.first);
            ASSERT_TRUE(itr != expectParams.end());
            ASSERT_EQ(kv.second, itr->second);
        }
    }
protected:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(aitheta_plugin, ParamUtilTest);

TEST_F(ParamUtilTest, testMergeParams) {
    KeyValueMap srcParams = {{"1", "what"}, {"2", "a"}, {"3", "fake"}, {"4", "day"}};
    KeyValueMap destParams = {{"1", "你"}, {"2", "说"}, {"3", "啥"}, {"4", "!"}, {"5", "笨"}};
    ParamUtil::MergeParams(srcParams, destParams);
    KeyValueMap expectParams = {{"1", "你"}, {"2", "说"}, {"3", "啥"}, {"4", "!"}, {"5", "笨"}};
    checkKeyValueMap(expectParams, destParams);
}

TEST_F(ParamUtilTest, testMergeParams1) {
    KeyValueMap srcParams = {{"1", "what"}, {"2", "a"}, {"3", "fake"}, {"4", "day"}, {"6", "!"}};
    KeyValueMap destParams = {{"1", "你"}, {"2", "说"}, {"3", "啥"}, {"4", "!"}, {"5", "笨"}};
    ParamUtil::MergeParams(srcParams, destParams);
    KeyValueMap expectParams = {{"1", "你"}, {"2", "说"}, {"3", "啥"}, {"4", "!"}, {"5", "笨"}, {"6", "!"}};
    checkKeyValueMap(expectParams, destParams);
}

TEST_F(ParamUtilTest, testMergeParamsCoverTrue) {
    KeyValueMap srcParams = {{"1", "what"}, {"2", "a"}, {"3", "fake"}, {"4", "day"}};
    KeyValueMap destParams = {{"1", "你"}, {"2", "说"}, {"3", "啥"}, {"4", "!"}, {"5", "笨"}};
    ParamUtil::MergeParams(srcParams, destParams, true);
    KeyValueMap expectParams = {{"1", "what"}, {"2", "a"}, {"3", "fake"}, {"4", "day"}, {"5", "笨"}};
    checkKeyValueMap(expectParams, destParams);
}


IE_NAMESPACE_END(aitheta_plugin);
