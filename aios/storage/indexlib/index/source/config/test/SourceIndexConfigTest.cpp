#include "indexlib/index/source/config/SourceIndexConfig.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/config/module_info.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class SourceIndexConfigTest : public TESTBASE
{
public:
    SourceIndexConfigTest() = default;
    ~SourceIndexConfigTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void SourceIndexConfigTest::setUp() {}

void SourceIndexConfigTest::tearDown() {}

TEST_F(SourceIndexConfigTest, testSimpleProcess)
{
    std::string configStr = R"(
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
                    "doc_compressor": "zlib|zstd",
                    "file_compress": "simple"
                }
            }
        ]
    })";

    SourceIndexConfig config;
    auto any = autil::legacy::json::ParseJson(configStr);
    MutableJson mtJson;

    auto fieldConfig1 = std::make_shared<FieldConfig>("ops_app_name", indexlib::FieldType::ft_string, false);
    auto fieldConfig2 = std::make_shared<FieldConfig>("ops_app_schema", indexlib::FieldType::ft_string, false);
    auto fieldConfig3 = std::make_shared<FieldConfig>("nid", indexlib::FieldType::ft_string, false);

    config::IndexConfigDeserializeResource resource({fieldConfig1, fieldConfig2, fieldConfig3}, mtJson);
    resource._fileCompressVec.reset(new std::vector<config::SingleFileCompressConfig>);
    config::SingleFileCompressConfig fileCompressConfig("simple", "zstd");
    resource._fileCompressVec->push_back(fileCompressConfig);
    config.Deserialize(any, 0, resource);

    ASSERT_NO_THROW(config.Check());

    ASSERT_EQ(3, config.GetSourceGroupCount());
    ASSERT_EQ(indexlib::config::SourceGroupConfig::SourceFieldMode::SPECIFIED_FIELD,
              config.GetGroupConfig(0)->GetFieldMode());
    ASSERT_EQ(indexlib::config::SourceGroupConfig::SourceFieldMode::USER_DEFINE,
              config.GetGroupConfig(1)->GetFieldMode());
    ASSERT_EQ(indexlib::config::SourceGroupConfig::SourceFieldMode::ALL_FIELD,
              config.GetGroupConfig(2)->GetFieldMode());
    // test set group id in source schema
    for (index::sourcegroupid_t i = 0; i < 3; ++i) {
        ASSERT_EQ(i, config.GetGroupConfig(i)->GetGroupId());
    }
    auto groupConfigWithCompress = config.GetGroupConfig(2);
    auto compressConfig = groupConfigWithCompress->GetParameter().GetFileCompressConfigV2();
    ASSERT_TRUE(compressConfig);
    ASSERT_EQ("simple", compressConfig->GetCompressName());

    ASSERT_EQ(0, config.GetGroupIdByFieldName("ops_app_name"));
    ASSERT_EQ(0, config.GetGroupIdByFieldName("ops_app_schema"));
    ASSERT_EQ(2, config.GetGroupIdByFieldName("nid"));
}

TEST_F(SourceIndexConfigTest, testDisableSourceGroup)
{
    std::string configStr = R"(
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
                "field_mode": "all_field"
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

    SourceIndexConfig config;
    auto any = autil::legacy::json::ParseJson(configStr);
    MutableJson mtJson;

    auto fieldConfig1 = std::make_shared<FieldConfig>("ops_app_name", indexlib::FieldType::ft_string, false);
    auto fieldConfig2 = std::make_shared<FieldConfig>("ops_app_schema", indexlib::FieldType::ft_string, false);

    config::IndexConfigDeserializeResource resource({fieldConfig1, fieldConfig2}, mtJson);
    config.Deserialize(any, 0, resource);

    ASSERT_NO_THROW(config.Check());

    ASSERT_EQ(3, config.GetSourceGroupCount());
    auto groupConfig = config.GetGroupConfig(0);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(0);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_FALSE(config.IsDisabled());

    groupConfig = config.GetGroupConfig(1);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(1);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_FALSE(config.IsDisabled());

    groupConfig = config.GetGroupConfig(2);
    ASSERT_FALSE(groupConfig->IsDisabled());
    config.DisableFieldGroup(2);
    ASSERT_TRUE(groupConfig->IsDisabled());
    ASSERT_TRUE(config.IsDisabled());
}

TEST_F(SourceIndexConfigTest, TestExceptionCase)
{
    {
        // not allow source config without group config
        std::string configStr = R"(
        {
            "modules": [
                {
                    "module_name": "test_module",
                    "module_path": "testpath",
                    "parameters": {"k1":"param1"}
                }
            ]
        })";
        SourceIndexConfig config;
        auto any = autil::legacy::json::ParseJson(configStr);
        MutableJson mtJson;
        config::IndexConfigDeserializeResource resource({}, mtJson);
        config.Deserialize(any, 0, resource);

        ASSERT_ANY_THROW(config.Check());
    }
}

} // namespace indexlibv2::config
