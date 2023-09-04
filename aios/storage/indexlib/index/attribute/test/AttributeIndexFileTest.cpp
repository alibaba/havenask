#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class AttributeIndexFileTest : public TESTBASE
{
public:
    AttributeIndexFileTest() = default;
    ~AttributeIndexFileTest() = default;
    void setUp() override {}
    void tearDown() override {}
    bool DoTestMatch(const std::string& pattern, const std::string& path);
};

bool AttributeIndexFileTest::DoTestMatch(const std::string& pattern, const std::string& path)
{
    file_system::LoadConfig loadConfig;
    file_system::LoadConfig::FilePatternStringVector filePatterns;
    filePatterns.push_back(pattern);
    loadConfig.SetFilePatternString(filePatterns);
    return loadConfig.Match(path, "");
}

TEST_F(AttributeIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("attribute", ATTRIBUTE_INDEX_PATH);
    ASSERT_EQ("patch", PATCH_FILE_NAME);
    ASSERT_EQ("data", ATTRIBUTE_DATA_FILE_NAME);
    ASSERT_EQ("offset", ATTRIBUTE_OFFSET_FILE_NAME);

    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/sub_segment)?";
    std::string attributePattern = segmentPattern + "/" + ATTRIBUTE_INDEX_PATH + "/";
    std::string attributeDataPattern = segmentPattern + "/attribute/\\w+/(slice_[0-9]+/)?" + ATTRIBUTE_DATA_FILE_NAME;
    std::string attributeOffsetPattern =
        segmentPattern + "/attribute/\\w+/(slice_[0-9]+/)?" + ATTRIBUTE_OFFSET_FILE_NAME;
    std::string attributePatchPattern = segmentPattern + "/(attribute|index)/\\w+/(\\w+/)?[0-9]+_[0-9]+\\.patch";
    ASSERT_EQ(attributePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_ATTRIBUTE_"));
    ASSERT_EQ(attributeDataPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_ATTRIBUTE_DATA_"));
    ASSERT_EQ(attributeOffsetPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_ATTRIBUTE_OFFSET_"));
    ASSERT_EQ(attributePatchPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_PATCH_"));
    // check slice file load config
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_DATA_", "segment_0/attribute/foo/slice_0/data"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_DATA_", "segment_0/attribute/foo/slice_100/data"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_OFFSET_", "segment_0/attribute/foo/slice_0/offset"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_OFFSET_", "segment_0/attribute/foo/slice_100/offset"));
}

} // namespace indexlib::index
