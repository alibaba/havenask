#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/adaptive_dictionary_config.h"
#include "indexlib/config/impl/adaptive_dictionary_config_impl.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/configurator_define.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);

class AdaptiveDictionaryConfigTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(AdaptiveDictionaryConfigTest);
    void CaseSetUp() override
    {
    }

    void CaseTearDown() override
    {
    }

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
        INDEXLIB_TEST_EQUAL("df", adaptiveDictConfig.GetRuleName());
        INDEXLIB_TEST_EQUAL(AdaptiveDictionaryConfig::DF_ADAPTIVE, 
                            adaptiveDictConfig.GetDictType());
        INDEXLIB_TEST_EQUAL((int32_t)200000, adaptiveDictConfig.GetThreshold());

        Any toAny = ToJson(adaptiveDictConfig);
        string toStr = ToString(toAny);
        INDEXLIB_TEST_EQUAL(dictStr, toStr);
    }

    void TestCaseForFromTypeString()
    {
        AdaptiveDictionaryConfig adaptiveDictConfig;
        AdaptiveDictionaryConfig::DictType result;
        result = adaptiveDictConfig.mImpl->FromTypeString("DOC_FREQUENCY");
        INDEXLIB_TEST_EQUAL(0, result);
        result = adaptiveDictConfig.mImpl->FromTypeString("PERCENT");
        INDEXLIB_TEST_EQUAL(1, result);
        result = adaptiveDictConfig.mImpl->FromTypeString("INDEX_SIZE");
        INDEXLIB_TEST_EQUAL(2, result);
        bool exception = false;
        try
        {
            result = adaptiveDictConfig.mImpl->FromTypeString("errorType");
        }
        catch (const misc::SchemaException& e) {
            exception = true;
        }
        INDEXLIB_TEST_TRUE(exception);
    }
    
    void TestCaseForToTypeString()
    {
        AdaptiveDictionaryConfig adaptiveDictConfig;
        string result;
        AdaptiveDictionaryConfig::DictType type = 
            AdaptiveDictionaryConfig::DF_ADAPTIVE;
        result = adaptiveDictConfig.mImpl->ToTypeString(type);
        INDEXLIB_TEST_EQUAL("DOC_FREQUENCY", result);
        type = AdaptiveDictionaryConfig::PERCENT_ADAPTIVE;
        result = adaptiveDictConfig.mImpl->ToTypeString(type);
        INDEXLIB_TEST_EQUAL("PERCENT", result);
        type = AdaptiveDictionaryConfig::SIZE_ADAPTIVE;
        result = adaptiveDictConfig.mImpl->ToTypeString(type);
        INDEXLIB_TEST_EQUAL("INDEX_SIZE", result);
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
            }
            catch (const misc::SchemaException& e) {
                exception = true;
            }
            INDEXLIB_TEST_TRUE(!exception);
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
            }
            catch (const misc::SchemaException& e) {
                exception = true;
            }
            INDEXLIB_TEST_TRUE(exception);
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
            }
            catch (const misc::SchemaException& e) {
                exception = true;
            }
            INDEXLIB_TEST_TRUE(exception);
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
            }
            catch (const misc::SchemaException& e) {
                exception = true;
            }
            INDEXLIB_TEST_TRUE(exception);
        }
    }
    
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, AdaptiveDictionaryConfigTest);

INDEXLIB_UNIT_TEST_CASE(AdaptiveDictionaryConfigTest, TestCaseForToTypeString);
INDEXLIB_UNIT_TEST_CASE(AdaptiveDictionaryConfigTest, TestCaseForJsonize);
INDEXLIB_UNIT_TEST_CASE(AdaptiveDictionaryConfigTest, TestCaseForCheckThreshold);
INDEXLIB_UNIT_TEST_CASE(AdaptiveDictionaryConfigTest, TestCaseForFromTypeString);

IE_NAMESPACE_END(config);
