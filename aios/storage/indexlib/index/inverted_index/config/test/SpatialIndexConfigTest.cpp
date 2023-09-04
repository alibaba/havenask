#include "indexlib/index/inverted_index/config/SpatialIndexConfig.h"

#include "autil/legacy/any.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/IndexConfigDeserializeResource.h"
#include "indexlib/config/MutableJson.h"
#include "indexlib/index/inverted_index/InvertedIndexFactory.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class SpatialIndexConfigTest : public TESTBASE
{
};

namespace {
std::shared_ptr<SpatialIndexConfig> CreateSpatialIndexConfig(const autil::legacy::Any& any)
{
    indexlib::index::InvertedIndexFactory factory;
    std::shared_ptr<IIndexConfig> indexConfig = factory.CreateIndexConfig(any);
    assert(indexConfig);
    const auto& spatialIndexConfig = std::dynamic_pointer_cast<SpatialIndexConfig>(indexConfig);
    assert(spatialIndexConfig);
    return spatialIndexConfig;
}
} // namespace

TEST_F(SpatialIndexConfigTest, testSimpleLoad)
{
    std::string jsonStr = R"(
    {
            "format_version_id" : 1,
            "file_compressor" : "zstd_compressor",
            "index_fields": "ops_geo_point_field_0",
            "index_name": "ops_geo_point_index_0",
            "index_type": "SPATIAL",
            "max_dist_err": 100,
            "max_search_dist": 10000
    }
    )";
    auto any = autil::legacy::json::ParseJson(jsonStr);
    std::vector<std::shared_ptr<FieldConfig>> fieldConfigs;
    auto fieldConfig = std::make_shared<FieldConfig>("ops_geo_point_field_0", ft_location, false);
    fieldConfig->SetFieldId(0);
    fieldConfigs.push_back(fieldConfig);
    auto checkConfig = [](const auto& config) {
        ASSERT_EQ(1, config->GetIndexFormatVersionId());
        const auto& compressConfig = config->GetFileCompressConfigV2();
        ASSERT_TRUE(compressConfig);
        ASSERT_EQ("zstd_compressor", compressConfig->GetCompressName());
        const auto& singleCompressConfigs = compressConfig->GetConfigs();
        ASSERT_EQ(2, singleCompressConfigs->size());
        {
            const auto& singleCompressConfig = (*singleCompressConfigs)[0];
            ASSERT_EQ("zstd_compressor", singleCompressConfig.GetCompressName());
            ASSERT_EQ("zstd", singleCompressConfig.GetCompressType());
        }
        {
            const auto& singleCompressConfig = (*singleCompressConfigs)[1];
            ASSERT_EQ("hot_compressor", singleCompressConfig.GetCompressName());
            ASSERT_EQ("", singleCompressConfig.GetCompressType());
        }
        auto fieldConfigs = config->GetFieldConfigs();
        ASSERT_EQ(1, fieldConfigs.size());
        auto fieldConfig = fieldConfigs[0];
        ASSERT_TRUE(fieldConfig);
        ASSERT_EQ("ops_geo_point_field_0", fieldConfig->GetFieldName());
        ASSERT_EQ(ft_location, fieldConfig->GetFieldType());
        ASSERT_TRUE(fieldConfig->IsMultiValue());
        ASSERT_EQ(0, fieldConfig->GetFieldId());
        ASSERT_EQ("ops_geo_point_index_0", config->GetIndexName());
        ASSERT_EQ(it_spatial, config->GetInvertedIndexType());
        ASSERT_EQ(100.0, config->GetMaxDistError());
        ASSERT_EQ(10000.0, config->GetMaxSearchDist());
    };
    SingleFileCompressConfig zstdCompressor(/*compressorName*/ "zstd_compressor", /*compressType*/ "zstd");
    SingleFileCompressConfig lz4Compressor(/*compressorName*/ "hot_compressor", /*compressType*/ "");
    SingleFileCompressConfigVec compressVec {zstdCompressor, lz4Compressor};
    MutableJson runtimeSettings;
    ASSERT_TRUE(runtimeSettings.SetValue(SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY, compressVec));
    IndexConfigDeserializeResource resource(fieldConfigs, runtimeSettings);
    auto config = CreateSpatialIndexConfig(any);
    config->Deserialize(any, 0, resource);
    config->Check();
    checkConfig(config);

    autil::legacy::Jsonizable::JsonWrapper json;
    config->Serialize(json);
    auto config2 = CreateSpatialIndexConfig(any);
    IndexConfigDeserializeResource resource2(fieldConfigs, runtimeSettings);
    config2->Deserialize(json.GetMap(), 0, resource);
    config2->Check();
    checkConfig(config2);
}

} // namespace indexlibv2::config
