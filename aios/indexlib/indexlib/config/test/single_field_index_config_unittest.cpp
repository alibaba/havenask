#include <autil/legacy/jsonizable.h>
#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"
#include "indexlib/config/dictionary_config.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/test/test.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(config);

class SingleFieldIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SingleFieldIndexConfigTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }
    
    void SetAdaptiveDictConfig(IndexConfig& indexConfig)
    {
        AdaptiveDictionaryConfigPtr adaptiveDict(
                new AdaptiveDictionaryConfig("dict", "DOC_FREQUENCY", 20));
        indexConfig.SetAdaptiveDictConfig(adaptiveDict);
    }

    void TestCaseForToJson()
    {
        SingleFieldIndexConfig testConfig("phrase", it_text);
        testConfig.SetHashTypedDictionary(true);
        DictionaryConfigPtr ptr(new DictionaryConfig("dicName", ""));
        testConfig.SetDictConfig(ptr);
        SetAdaptiveDictConfig(testConfig);
        string str = ToJsonString(testConfig);
        SingleFieldIndexConfig testConfigNew("phrase", it_text);
        FromJsonString(testConfigNew, str);
        ASSERT_NO_THROW(testConfigNew.AssertEqual(testConfig));
        ASSERT_TRUE(testConfigNew.IsHashTypedDictionary());
    }

private:
    string LoadJsonString(const string& configFile)
    {
        ifstream in(configFile.c_str());
        if (!in)
        {
            IE_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", configFile.c_str());
            return "";
        }
        string line;
        string jsonString;
        while (getline(in, line))
        {
            jsonString += line;
        }

        IE_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
        return jsonString;
    }

private:
    string mRootDir;
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, SingleFieldIndexConfigTest);
INDEXLIB_UNIT_TEST_CASE(SingleFieldIndexConfigTest, TestCaseForToJson);

IE_NAMESPACE_END(config);
