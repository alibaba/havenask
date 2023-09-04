#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"

#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {

class AdaptiveDictionaryConfigTest : public TESTBASE
{
public:
    void TestCaseForJsonize()
    {
        string dictStr = "{\n\
\"adaptive_dictionary_name\":\n\
  \"df\",\n\
\"dict_type\":\n\
  \"DOC_FREQUENCY\",\n\
\"threshold\":\n\
  200000\n\
}";

        AdaptiveDictionaryConfig adaptiveDictConfig;
        Any any = ParseJson(dictStr);
        FromJson(adaptiveDictConfig, any);
        ASSERT_EQ("df", adaptiveDictConfig.GetRuleName());
        ASSERT_EQ(AdaptiveDictionaryConfig::DF_ADAPTIVE, adaptiveDictConfig.GetDictType());
        ASSERT_EQ((int32_t)200000, adaptiveDictConfig.GetThreshold());

        Any toAny = ToJson(adaptiveDictConfig);
        string toStr = ToString(toAny);
        ASSERT_EQ(dictStr, toStr);
    }

    void TestCaseForFromTypeString()
    {
        AdaptiveDictionaryConfig adaptiveDictConfig;
        AdaptiveDictionaryConfig::DictType result;
        result = adaptiveDictConfig.FromTypeString("DOC_FREQUENCY");
        ASSERT_EQ(0, result);
        result = adaptiveDictConfig.FromTypeString("PERCENT");
        ASSERT_EQ(1, result);
        result = adaptiveDictConfig.FromTypeString("INDEX_SIZE");
        ASSERT_EQ(2, result);
        bool exception = false;
        try {
            result = adaptiveDictConfig.FromTypeString("errorType");
        } catch (const util::SchemaException& e) {
            exception = true;
        }
        ASSERT_TRUE(exception);
    }

    void TestCaseForToTypeString()
    {
        AdaptiveDictionaryConfig adaptiveDictConfig;
        string result;
        AdaptiveDictionaryConfig::DictType type = AdaptiveDictionaryConfig::DF_ADAPTIVE;
        result = adaptiveDictConfig.ToTypeString(type);
        ASSERT_EQ("DOC_FREQUENCY", result);
        type = AdaptiveDictionaryConfig::PERCENT_ADAPTIVE;
        result = adaptiveDictConfig.ToTypeString(type);
        ASSERT_EQ("PERCENT", result);
        type = AdaptiveDictionaryConfig::SIZE_ADAPTIVE;
        result = adaptiveDictConfig.ToTypeString(type);
        ASSERT_EQ("INDEX_SIZE", result);
    }

    void TestCaseForCheckThreshold()
    {
        {
            bool exception = false;
            try {
                string dictStr = "{ \"adaptive_dictionary_name\":\"df\", \
                                    \"dict_type\":\"DOC_FREQUENCY\",     \
                                    \"threshold\": 10                    \
                                  }";

                AdaptiveDictionaryConfig adaptiveDictConfig;
                Any any = ParseJson(dictStr);
                FromJson(adaptiveDictConfig, any);
            } catch (const util::SchemaException& e) {
                exception = true;
            }
            ASSERT_TRUE(!exception);
        }

        {
            bool exception = false;
            try {
                string dictStr = "{ \"adaptive_dictionary_name\":\"df\", \
                                    \"dict_type\":\"DOC_FREQUENCY\",     \
                                    \"threshold\": -1                    \
                                  }";

                AdaptiveDictionaryConfig adaptiveDictConfig;
                Any any = ParseJson(dictStr);
                FromJson(adaptiveDictConfig, any);
            } catch (const util::SchemaException& e) {
                exception = true;
            }
            ASSERT_TRUE(exception);
        }

        {
            bool exception = false;
            try {
                string dictStr = "{ \"adaptive_dictionary_name\":\"ruleName\", \
                                    \"dict_type\":\"PERCENT\",          \
                                    \"threshold\": -1                   \
                                  }";

                AdaptiveDictionaryConfig adaptiveDictConfig;
                Any any = ParseJson(dictStr);
                FromJson(adaptiveDictConfig, any);
            } catch (const util::SchemaException& e) {
                exception = true;
            }
            ASSERT_TRUE(exception);
        }

        {
            bool exception = false;
            try {
                string dictStr = "{ \"adaptive_dictionary_name\":\"ruleName\", \
                                    \"dict_type\":\"PERCENT\",          \
                                    \"threshold\": 101                  \
                                  }";

                AdaptiveDictionaryConfig adaptiveDictConfig;
                Any any = ParseJson(dictStr);
                FromJson(adaptiveDictConfig, any);
            } catch (const util::SchemaException& e) {
                exception = true;
            }
            ASSERT_TRUE(exception);
        }
    }
};

TEST_F(AdaptiveDictionaryConfigTest, TestCaseForToTypeString) { TestCaseForToTypeString(); }
TEST_F(AdaptiveDictionaryConfigTest, TestCaseForJsonize) { TestCaseForJsonize(); }
TEST_F(AdaptiveDictionaryConfigTest, TestCaseForCheckThreshold) { TestCaseForCheckThreshold(); }
TEST_F(AdaptiveDictionaryConfigTest, TestCaseForFromTypeString) { TestCaseForFromTypeString(); }
}} // namespace indexlib::config
