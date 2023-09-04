#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/source/Common.h"
#include "indexlib/index/source/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class SourceIndexFileTest : public TESTBASE
{
public:
    SourceIndexFileTest() = default;
    ~SourceIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(SourceIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("source", std::string(SOURCE_INDEX_PATH));
    ASSERT_EQ("group", std::string(SOURCE_DATA_DIR_PREFIX));
    ASSERT_EQ("data", std::string(SOURCE_DATA_FILE_NAME));
    ASSERT_EQ("offset", std::string(SOURCE_OFFSET_FILE_NAME));
    ASSERT_EQ("meta", std::string(SOURCE_META_DIR));

    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/sub_segment)?";
    std::string sourcePattern = segmentPattern + "/" + SOURCE_INDEX_PATH + "/";
    std::string sourceDataPattern =
        sourcePattern + index::SOURCE_DATA_DIR_PREFIX + "_[0-9]+/" + index::SOURCE_DATA_FILE_NAME + "$";
    std::string sourceMetaPattern = sourcePattern + index::SOURCE_META_DIR + "/" + index::SOURCE_DATA_FILE_NAME + "$";
    std::string sourceOffsetPattern =
        sourcePattern + index::SOURCE_DATA_DIR_PREFIX + "_[0-9]+/" + index::SOURCE_OFFSET_FILE_NAME + "$";
    ASSERT_EQ(sourcePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SOURCE_"));
    ASSERT_EQ(sourceDataPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SOURCE_DATA_"));
    ASSERT_EQ(sourceMetaPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SOURCE_META_"));
    ASSERT_EQ(sourceOffsetPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SOURCE_OFFSET_"));
}

} // namespace indexlib::index
