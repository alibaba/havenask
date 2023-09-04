#include "indexlib/index/common/FileCompressParamHelper.h"

#include "unittest/unittest.h"

namespace indexlibv2::index {

class FileCompressParamHelperTest : public TESTBASE
{
public:
    FileCompressParamHelperTest() = default;
    ~FileCompressParamHelperTest() = default;

public:
    void setUp() override;
    void tearDown() override;
};

void FileCompressParamHelperTest::setUp() {}

void FileCompressParamHelperTest::tearDown() {}

TEST_F(FileCompressParamHelperTest, testGetCompressConfig)
{
    std::shared_ptr<std::vector<config::SingleFileCompressConfig>> configs;
    std::string configStr = R"([
{
"name":"hot_compressor",
"type":"zstd"
},
{
"name":"cold_compressor",
"type":"snappy"
},
{
"name":"temperature",
"type":"combined",
"statistic_key":"segment_group",
"statistic_values":["hot", "cold"],
"compressor_names":["hot_compressor", "cold_compressor"]
}
])";
    autil::legacy::FromJsonString(configs, configStr);
    ASSERT_EQ(3, configs->size());

    {
        std::shared_ptr<config::FileCompressConfigV2> fileCompressConfig(
            new config::FileCompressConfigV2(configs, "hot_compressor"));
        auto config = FileCompressParamHelper::GetCompressConfig(fileCompressConfig, nullptr);
        ASSERT_EQ("zstd", config.GetCompressType());
    }

    {
        std::shared_ptr<framework::SegmentStatistics> segmentStats;
        std::shared_ptr<config::FileCompressConfigV2> fileCompressConfig(
            new config::FileCompressConfigV2(configs, "temperature"));
        auto config = FileCompressParamHelper::GetCompressConfig(fileCompressConfig, segmentStats);
        ASSERT_EQ("", config.GetCompressType());
        segmentStats.reset(new framework::SegmentStatistics);
        auto config2 = FileCompressParamHelper::GetCompressConfig(fileCompressConfig, segmentStats);
        ASSERT_EQ("", config2.GetCompressType());

        segmentStats->AddStatistic("segment_group", "hot");
        auto config3 = FileCompressParamHelper::GetCompressConfig(fileCompressConfig, segmentStats);
        ASSERT_EQ("zstd", config3.GetCompressType());
    }
}
} // namespace indexlibv2::index
