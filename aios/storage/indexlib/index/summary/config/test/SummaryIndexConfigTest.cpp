#include "indexlib/index/summary/config/SummaryIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

class SummaryIndexConfigTest : public TESTBASE
{
public:
    void setUp() override {};
    void tearDown() override {};
};

TEST_F(SummaryIndexConfigTest, testSimpleLoad)
{
    std::string jsonStr = R"(
    {
         "summary_fields": [ "nid", "title"],
         "parameter" : {
              "doc_compressor" : "zlib",
              "file_compressor" : "zstd_compressor"
         },
         "summary_groups" : [
             {
                "group_name" : "mainse",
                "parameter": {
                    "doc_compressor" : "zstd",
                    "file_compressor": "lz4_compressor"
                },
                "summary_fields" : ["quantity" ]
             },
             {
                "group_name" : "inshop",
                "summary_fields" : [ "user", "nick"]
             }
         ]
    }
    )";

    auto any = ParseJson(jsonStr);
    std::vector<std::pair<std::string, FieldType>> fields {
        {"nid", ft_int32}, {"title", ft_string}, {"quantity", ft_int32}, {"user", ft_int8}, {"nick", ft_string}};
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    fieldid_t fieldId = 0;
    for (const auto& [name, type] : fields) {
        auto config = std::make_shared<FieldConfig>(name, type, /*multiValue*/ false);
        config->SetFieldId(fieldId++);
        fieldConfigs.push_back(config);
    }
    SingleFileCompressConfig zstdCompressor(/*compressorName*/ "zstd_compressor", /*compressType*/ "zstd");
    SingleFileCompressConfig lz4Compressor(/*compressorName*/ "lz4_compressor", /*compressType*/ "lz4");
    SingleFileCompressConfigVec compressVec {zstdCompressor, lz4Compressor};
    MutableJson runtimeSettings;
    ASSERT_TRUE(runtimeSettings.SetValue(SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY, compressVec));
    auto checkConfig = [&](const auto& config) {
        ASSERT_EQ(indexlibv2::index::SUMMARY_INDEX_NAME, config.GetIndexName());
        auto actualFieldConfigs = config.GetFieldConfigs();
        ASSERT_EQ(fields.size(), actualFieldConfigs.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            EXPECT_EQ(fields[i].first, actualFieldConfigs[i]->GetFieldName());
            EXPECT_EQ(fields[i].second, actualFieldConfigs[i]->GetFieldType());
        }
        ASSERT_EQ(fields.size(), config.GetSummaryCount());
        ASSERT_EQ(3, config.GetSummaryGroupConfigCount());

        // TODO: test fileCompressConfig
        {
            auto group = config.GetSummaryGroupConfig(index::DEFAULT_SUMMARYGROUPNAME);
            ASSERT_TRUE(group);
            ASSERT_TRUE(group->IsDefaultGroup());
            ASSERT_EQ(0, group->GetGroupId());
            ASSERT_TRUE(group->IsCompress());
            ASSERT_EQ("zlib", group->GetCompressType());
            ASSERT_EQ(2u, group->GetSummaryFieldsCount());
            ASSERT_EQ("zstd_compressor",
                      group->GetSummaryGroupDataParam().GetFileCompressConfigV2()->GetCompressName());
        }
        {
            auto group = config.GetSummaryGroupConfig("mainse");
            ASSERT_TRUE(group);
            ASSERT_FALSE(group->IsDefaultGroup());
            ASSERT_EQ(1, group->GetGroupId());
            ASSERT_TRUE(group->IsCompress());
            ASSERT_EQ(1u, group->GetSummaryFieldsCount());
            ASSERT_EQ("lz4_compressor", group->GetSummaryGroupDataParam().GetFileCompressConfigV2()->GetCompressName());
        }
        {
            auto group = config.GetSummaryGroupConfig("inshop");
            ASSERT_TRUE(group);
            ASSERT_FALSE(group->IsDefaultGroup());
            ASSERT_EQ(2, group->GetGroupId());
            ASSERT_FALSE(group->IsCompress());
            ASSERT_EQ(2u, group->GetSummaryFieldsCount());
            ASSERT_FALSE(group->GetSummaryGroupDataParam().GetFileCompressConfigV2());
        }
    };
    SummaryIndexConfig config;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    config.Deserialize(any, 0, resource);
    config.Check();
    checkConfig(config);

    autil::legacy::Jsonizable::JsonWrapper json;
    config.Serialize(json);
    SummaryIndexConfig config2;
    IndexConfigDeserializeResource resource2(fieldConfigs, runtimeSettings);
    config2.Deserialize(json.GetMap(), 0, resource);
    config2.Check();
    checkConfig(config2);
}

TEST_F(SummaryIndexConfigTest, testNeedStore)
{
    std::string jsonStr = R"(
    {
         "summary_fields": [ "nid", "title"],
         "summary_groups" : [
             {
                "group_name" : "mainse",
                "summary_fields" : ["quantity" ]
             },
             {
                "group_name" : "inshop",
                "summary_fields" : [ "user", "nick"]
             }
         ]
    }
    )";

    auto any = ParseJson(jsonStr);
    std::vector<std::pair<std::string, FieldType>> fields {
        {"nid", ft_int32}, {"title", ft_string}, {"quantity", ft_int32}, {"user", ft_int8}, {"nick", ft_string}};
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    fieldid_t fieldId = 0;
    for (const auto& [name, type] : fields) {
        auto config = std::make_shared<FieldConfig>(name, type, /*multiValue*/ false);
        config->SetFieldId(fieldId++);
        fieldConfigs.push_back(config);
    }

    SummaryIndexConfig config;
    MutableJson runtimeSettings;
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    config.Deserialize(any, 0, resource);
    config.Check();
    config.SetNeedStoreSummary(false);

    for (fieldid_t fieldId = 0; fieldId < 5; ++fieldId) {
        ASSERT_FALSE(config.NeedStoreSummary(fieldId));
    }
    ASSERT_FALSE(config.NeedStoreSummary());

    config.SetNeedStoreSummary(0);
    for (fieldid_t fieldId = 1; fieldId < 5; ++fieldId) {
        ASSERT_FALSE(config.NeedStoreSummary(fieldId));
    }
    ASSERT_TRUE(config.NeedStoreSummary(0));
    ASSERT_TRUE(config.NeedStoreSummary());

    config.SetNeedStoreSummary(false);
    for (fieldid_t fieldId = 0; fieldId < 5; ++fieldId) {
        ASSERT_FALSE(config.NeedStoreSummary(fieldId));
    }
    ASSERT_FALSE(config.NeedStoreSummary());
}

} // namespace indexlibv2::config
