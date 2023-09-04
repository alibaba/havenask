#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/config/SectionAttributeConfig.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class InvertedIndexFileTest : public TESTBASE
{
public:
    InvertedIndexFileTest() = default;
    ~InvertedIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(InvertedIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("index", INVERTED_INDEX_PATH);
    ASSERT_EQ("attribute", ATTRIBUTE_INDEX_PATH);
    ASSERT_EQ("patch", PATCH_FILE_NAME);
    ASSERT_EQ("dictionary", DICTIONARY_FILE_NAME);
    ASSERT_EQ("posting", POSTING_FILE_NAME);
    ASSERT_EQ("_section", indexlibv2::config::SectionAttributeConfig::SECTION_DIR_NAME_SUFFIX);
    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/sub_segment)?";
    std::string patchPattern = segmentPattern + "/(" + ATTRIBUTE_INDEX_PATH + "|" + INVERTED_INDEX_PATH +
                               ")/\\w+/(\\w+/)?[0-9]+_[0-9]+\\.patch";
    std::string indexPattern = segmentPattern + "/index/";
    std::string sectionAttributePattern = segmentPattern + "/index/\\w+_section/";
    std::string dictionaryPattern = segmentPattern + "/index/\\w+/" + DICTIONARY_FILE_NAME;
    std::string postingPattern = segmentPattern + "/index/\\w+/" + POSTING_FILE_NAME;
    ASSERT_EQ(patchPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_PATCH_"));
    ASSERT_EQ(sectionAttributePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_SECTION_ATTRIBUTE_"));
    ASSERT_EQ(indexPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_INDEX_"));
    ASSERT_EQ(dictionaryPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_INDEX_DICTIONARY_"));
    ASSERT_EQ(postingPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_INDEX_POSTING_"));
}

} // namespace indexlib::index
