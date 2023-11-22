#include <fstream>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/config/AdaptiveDictionaryConfig.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace indexlib::file_system;
namespace indexlib { namespace config {

class SingleFieldIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(SingleFieldIndexConfigTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void SetAdaptiveDictConfig(IndexConfig& indexConfig)
    {
        std::shared_ptr<AdaptiveDictionaryConfig> adaptiveDict(
            new AdaptiveDictionaryConfig("dict", "DOC_FREQUENCY", 20));
        indexConfig.SetAdaptiveDictConfig(adaptiveDict);
    }

    void TestCaseForToJson()
    {
        SingleFieldIndexConfig testConfig("phrase", it_text);
        FieldConfigPtr fieldConfig(new FieldConfig("f1", ft_text, false));
        fieldConfig->SetUserDefinedParam(util::ConvertToJsonMap({{"dict_hash_func", "LayerHash"}, {"seperator", "#"}}));
        testConfig.SetFieldConfig(fieldConfig);
        ASSERT_FALSE(fieldConfig->GetUserDefinedParam().empty());
        ASSERT_EQ(util::ConvertFromJsonMap(fieldConfig->GetUserDefinedParam()), testConfig.GetDictHashParams());

        testConfig.SetHashTypedDictionary(true);
        std::shared_ptr<DictionaryConfig> ptr(new DictionaryConfig("dicName", ""));
        testConfig.SetDictConfig(ptr);
        SetAdaptiveDictConfig(testConfig);

        string str = ToJsonString(testConfig);
        SingleFieldIndexConfig testConfigNew("phrase", it_text);
        FromJsonString(testConfigNew, str);
        ASSERT_NO_THROW(testConfigNew.AssertEqual(testConfig));
        ASSERT_TRUE(testConfigNew.IsHashTypedDictionary());
    }

    void TestInvalidIndexTypeForEnableNull()
    {
        {
            SingleFieldIndexConfig testConfig("index", it_range);
            FieldConfigPtr fieldConfig(new FieldConfig("field", ft_int64, false));
            fieldConfig->SetEnableNullField(true);
            ASSERT_ANY_THROW(testConfig.SetFieldConfig(fieldConfig));
        }
    }

private:
    string LoadJsonString(const string& configFile)
    {
        ifstream in(configFile.c_str());
        if (!in) {
            AUTIL_LOG(ERROR, "OPEN INPUT FILE[%s] ERROR", configFile.c_str());
            return "";
        }
        string line;
        string jsonString;
        while (getline(in, line)) {
            jsonString += line;
        }

        AUTIL_LOG(DEBUG, "jsonString:%s", jsonString.c_str());
        return jsonString;
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(index, SingleFieldIndexConfigTest);
INDEXLIB_UNIT_TEST_CASE(SingleFieldIndexConfigTest, TestCaseForToJson);
INDEXLIB_UNIT_TEST_CASE(SingleFieldIndexConfigTest, TestInvalidIndexTypeForEnableNull);
}} // namespace indexlib::config
