#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class SummaryIndexFileTest : public TESTBASE
{
public:
    SummaryIndexFileTest() = default;
    ~SummaryIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(SummaryIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("summary", SUMMARY_INDEX_NAME);
    ASSERT_EQ("data", SUMMARY_DATA_FILE_NAME);
    ASSERT_EQ("offset", SUMMARY_OFFSET_FILE_NAME);

    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/sub_segment)?";
    std::string summaryPattern = segmentPattern + "/" + SUMMARY_INDEX_NAME + "/";
    std::string summaryDataPattern = summaryPattern + index::SUMMARY_DATA_FILE_NAME + "$";
    std::string summaryOffsetPattern = summaryPattern + index::SUMMARY_OFFSET_FILE_NAME + "$";
    ASSERT_EQ(summaryPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SUMMARY_"));
    ASSERT_EQ(summaryDataPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SUMMARY_DATA_"));
    ASSERT_EQ(summaryOffsetPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SUMMARY_OFFSET_"));
}

} // namespace indexlib::index
