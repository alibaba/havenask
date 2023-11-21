#include "autil/Log.h"
#include "indexlib/config/impl/source_schema_impl.h"
#include "indexlib/config/source_schema.h"
#include "indexlib/util/testutil/unittest.h"

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestExceptionCase);
INDEXLIB_UNIT_TEST_CASE(SourceSchemaTest, TestDisableSourceGroup);

AUTIL_LOG_SETUP(indexlib.config, SourceSchemaTest);

SourceSchemaTest::SourceSchemaTest() {}

SourceSchemaTest::~SourceSchemaTest() {}

void SourceSchemaTest::CaseSetUp() {}

void SourceSchemaTest::CaseTearDown() {}

void SourceSchemaTest::TestSimpleProcess()
{
    string configStr = R"(
    {
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["ops_app_name", "ops_app_schema"],
                "parameter" : {
                    "compress_type": "uniq"
                }
            },
            {
                "field_mode": "user_define"
            },
            {
                "field_mode": "all_field",
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
    ASSERT_EQ(3, configImpl->mGroupConfigs.size());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD, configImpl->mGroupConfigs[0]->GetFieldMode());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::USER_DEFINE, configImpl->mGroupConfigs[1]->GetFieldMode());
    ASSERT_EQ(SourceGroupConfig::SourceFieldMode::ALL_FIELD, configImpl->mGroupConfigs[2]->GetFieldMode());
    // test set group id in source schema
    for (index::sourcegroupid_t i = 0; i < 3; ++i) {
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
        "group_configs": [
            {
                "field_mode": "specified_field",
                "fields": ["ops_app_name", "ops_app_schema"],
                "parameter" : {
                    "compress_type": "uniq"
                }
            },
            {
                "field_mode": "user_define"
            },
            {
                "field_mode": "all_field",
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
        // not allow source schema without group config
        string configStr = R"(
        {
        })";
        SourceSchema config;
        Any any = ParseJson(configStr);
        FromJson(config, any);
        ASSERT_ANY_THROW(config.Check());
    }
}
}} // namespace indexlib::config
