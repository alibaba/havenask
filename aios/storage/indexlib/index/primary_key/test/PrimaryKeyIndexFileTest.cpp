#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class PrimaryKeyIndexFileTest : public TESTBASE
{
public:
    PrimaryKeyIndexFileTest() = default;
    ~PrimaryKeyIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(PrimaryKeyIndexFileTest, TestLoadConfig)
{
    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/sub_segment)?";
    std::string pkAttributePattern = segmentPattern + "/index/\\w+/attribute_";
    ASSERT_EQ(pkAttributePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_PK_ATTRIBUTE_"));
}

} // namespace indexlib::index
