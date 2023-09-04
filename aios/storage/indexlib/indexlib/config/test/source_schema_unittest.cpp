#include "indexlib/common_define.h"
#include "indexlib/config/impl/source_schema_impl.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib { namespace config {

class SourceSchemaTest : public INDEXLIB_TESTBASE
{
public:
    SourceSchemaTest();
    ~SourceSchemaTest();

    DECLARE_CLASS_NAME(SourceSchemaTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestExceptionCase();
    void TestDisableSourceGroup();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestExceptionCase);
INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestDisableSourceGroup);

IE_LOG_SETUP(config, SourceSchemaTest);

SourceSchemaTest::SourceSchemaTest() {}

SourceSchemaTest::~SourceSchemaTest() {}

void SourceSchemaTest::CaseSetUp() {}

void SourceSchemaTest::CaseTearDown() {}

void SourceSchemaTest::TestSimpleProcess()
{
    string configStr = R"(
    {
        "modules": [
            {
                "module_name": "test_module",
                "module_path": "testpath",
                "parameters": {"k1":"param1"}
            },
            {
                "module_name": "test_module2",
                "module_path": "testpath2",
                "parameters": {"k2":"param2"}
            }
        ],
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["ops_app_name", "ops_app_schema"],
                "parameter" : {
                    "compress_type": "uniq"
                }
            },
            {
                "field_mode": "user_define",
                "module": "test_module"
            },
            {
                "field_mode": "all_field",
                "module": "test_module2",
                "parameter" : {
                    "compress_type": "equal",
                    "doc_compressor": "zlib|zstd"
                }
            }
        ]
    })";

    SourceSchema config;
    Any any = ParseJson(configStr);
    FromJson(config, any);

    ASSERT_NO_THROW(config.Check());
    SourceSchemaImplPtr configImpl = config.mImpl;
    ASSERT_EQ(2, configImpl->mModules.size());
    ASSERT_EQ("test_module", configImpl->mModules[0].moduleName);
    ASSERT_EQ("testpath", configImpl->mModules[0].modulePath);
    ASSERT_EQ(3, configImpl->mGroupConfigs.size());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD, configImpl->mGroupConfigs[0]->GetFieldMode());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::USER_DEFINE, configImpl->mGroupConfigs[1]->GetFieldMode());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::ALL_FIELD, configImpl->mGroupConfigs[2]->GetFieldMode());
    // test set group id in source schema
    for (index::groupid_t i = 0; i < 3; ++i) {
        ASSERT_EQ(i, configImpl->mGroupConfigs[i]->GetGroupId());
    }
    Any toAny = ToJson(config);
    string jsonString = ToString(toAny);
    Any comparedAny = ParseJson(jsonString);
    SourceSchema comparedConfig;
    FromJson(comparedConfig, toAny);
    ASSERT_NO_THROW(config.AssertEqual(comparedConfig));
}

void SourceSchemaTest::TestDisableSourceGroup()
{
    string configStr = R"(
    {
        "modules": [
            {
                "module_name": "test_module",
                "module_path": "testpath",
                "parameters": {"k1":"param1"}
            },
            {
                "module_name": "test_module2",
                "module_path": "testpath2",
                "parameters": {"k2":"param2"}
            }
        ],
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["ops_app_name", "ops_app_schema"],
                "parameter" : {
                    "compress_type": "uniq"
                }
            },
            {
                "field_mode": "user_define",
                "module": "test_module"
            },
            {
                "field_mode": "all_field",
                "module": "test_module2",
                "parameter" : {
                    "compress_type": "equal",
                    "doc_compressor": "zlib|zstd"
                }
            }
        ]
    })";

    SourceSchema config;
    Any any = ParseJson(configStr);
    FromJson(config, any);
    ASSERT_NO_THROW(config.Check());

    ASSERT_EQ(3, config.GetSourceGroupCount());
    auto groupConfig = config.GetGroupConfig(0);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(0);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_FALSE(config.IsAllFieldsDisabled());

    groupConfig = config.GetGroupConfig(1);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(1);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_FALSE(config.IsAllFieldsDisabled());

    groupConfig = config.GetGroupConfig(2);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(2);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_TRUE(config.IsAllFieldsDisabled());
}

void SourceSchemaTest::TestExceptionCase()
{
    {
        // not allow group config use non-existed module
        string configStr = R"(
        {
            "modules": [
                {
                    "module_name": "test_module",
                    "module_path": "testpath",
                    "parameters": {"k1":"param1"}
                }
            ],
            "group_configs": [
                {
                    "field_mode": "user_define",
                    "module": "module_none_exist"
                }
            ]
        })";
        SourceSchema config;
        Any any = ParseJson(configStr);
        FromJson(config, any);
        ASSERT_ANY_THROW(config.Check());
    }
    {
        // not allow source schema without group config
        string configStr = R"(
        {
            "modules": [
                {
                    "module_name": "test_module",
                    "module_path": "testpath",
                    "parameters": {"k1":"param1"}
                }
            ]
        })";
        SourceSchema config;
        Any any = ParseJson(configStr);
        FromJson(config, any);
        ASSERT_ANY_THROW(config.Check());
    }
}
}} // namespace indexlib::config
