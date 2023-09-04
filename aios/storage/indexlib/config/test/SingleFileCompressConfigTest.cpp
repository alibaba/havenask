#include "indexlib/config/SingleFileCompressConfig.h"

#include "indexlib/config/TabletSchema.h"
#include "unittest/unittest.h"

namespace indexlibv2::config {

class SingleFileCompressConfigTest : public TESTBASE
{
public:
    SingleFileCompressConfigTest() = default;
    ~SingleFileCompressConfigTest() = default;

public:
    void setUp() override {};
    void tearDown() override {};
};

TEST_F(SingleFileCompressConfigTest, TestJsonize)
{
    std::string jsonStr = R"(
     [
            {
                "name":"hot_compressor",
                "type":"zstd"
            },
            {
                "name":"cold_compressor",
                "type":"snappy"
            },
            {
                "name":"combined_compressor",
                "type":"combined",
                "statistic_key":"segment_group",
                "statistic_values": ["hot", "cold"],
                "compressor_names": ["hot_compressor", "cold_compressor"],
                "default_compressor":"cold_compressor"
            }
      ]
)";
    SingleFileCompressConfigVec fileCompressConfigs;
    FromJsonString(fileCompressConfigs, jsonStr);
    ASSERT_EQ(3, fileCompressConfigs.size());
    {
        auto compressConfig = fileCompressConfigs[0];
        ASSERT_EQ("hot_compressor", compressConfig.GetCompressName());
        ASSERT_EQ("zstd", compressConfig.GetCompressType());
    }
    {
        auto compressConfig = fileCompressConfigs[1];
        ASSERT_EQ("cold_compressor", compressConfig.GetCompressName());
        ASSERT_EQ("snappy", compressConfig.GetCompressType());
    }
    {
        auto compressConfig = fileCompressConfigs[2];
        ASSERT_EQ("combined_compressor", compressConfig.GetCompressName());
        ASSERT_EQ("combined", compressConfig.GetCompressType());
        ASSERT_EQ("segment_group", compressConfig.GetStatisticKey());
        ASSERT_EQ(std::vector<std::string>({"hot", "cold"}), compressConfig.GetStatisticValues());
        ASSERT_EQ(std::vector<std::string>({"hot_compressor", "cold_compressor"}), compressConfig.GetCompressorNames());
        ASSERT_EQ("cold_compressor", compressConfig.GetDefaultCompressor());
    }
}

TEST_F(SingleFileCompressConfigTest, TestInvalidConfig)
{
    {
        SingleFileCompressConfig config;
        // value和name个数不一样
        std::string configStr = R"({
            "name": "name",
            "type": "combined",
            "statistic_key":"key",
            "statistic_values":[],
            "compressor_names":["c1"]
        })";

        autil::legacy::FromJsonString(config, configStr);
        std::vector<SingleFileCompressConfig> configs({config});
        ASSERT_FALSE(SingleFileCompressConfig::ValidateConfigs(configs).IsOK());
    }
    {
        // 不合法的压缩方式
        SingleFileCompressConfig config;
        std::string configStr = R"({
            "name": "name",
            "type": "12345"
        })";
        autil::legacy::FromJsonString(config, configStr);
        std::vector<SingleFileCompressConfig> configs({config});
        ASSERT_FALSE(SingleFileCompressConfig::ValidateConfigs(configs).IsOK());
    }

    {
        // 没有name
        SingleFileCompressConfig config;
        std::string configStr = R"({
            "name": "",
            "type": "zstd"
        })";
        autil::legacy::FromJsonString(config, configStr);
        std::vector<SingleFileCompressConfig> configs({config});
        ASSERT_FALSE(SingleFileCompressConfig::ValidateConfigs(configs).IsOK());
    }
    {
        // combined策略没有对应的压缩方式
        SingleFileCompressConfig config;
        std::string configStr = R"({
            "name": "name",
            "type": "combined",
            "statistic_key":"key",
            "statistic_values":["v1"],
            "compressor_names":["c1"]
        })";
        autil::legacy::FromJsonString(config, configStr);
        std::vector<SingleFileCompressConfig> configs({config});
        ASSERT_FALSE(SingleFileCompressConfig::ValidateConfigs(configs).IsOK());
    }
    {
        // combined引用了combined
        SingleFileCompressConfig config1;
        std::string configStr1 = R"({
            "name": "name1",
            "type": "combined",
            "statistic_key":"key",
            "statistic_values":["v1"],
            "compressor_names":["name2"]
        })";
        SingleFileCompressConfig config2;
        std::string configStr2 = R"({
            "name": "name2",
            "type": "combined",
            "statistic_key":"key",
            "statistic_values":[],
            "compressor_names":[]
        })";
        autil::legacy::FromJsonString(config1, configStr1);
        autil::legacy::FromJsonString(config2, configStr2);
        std::vector<SingleFileCompressConfig> configs({config1, config2});
        ASSERT_FALSE(SingleFileCompressConfig::ValidateConfigs(configs).IsOK());
    }
}

} // namespace indexlibv2::config
