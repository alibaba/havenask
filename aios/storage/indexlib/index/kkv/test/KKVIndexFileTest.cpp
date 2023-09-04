#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/kkv/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class KKVIndexFileTest : public TESTBASE
{
public:
    KKVIndexFileTest() = default;
    ~KKVIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(KKVIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("index", std::string(KKV_INDEX_PATH));
    ASSERT_EQ("pkey", std::string(PREFIX_KEY_FILE_NAME));
    ASSERT_EQ("skey", std::string(SUFFIX_KEY_FILE_NAME));
    ASSERT_EQ("value", std::string(KKV_VALUE_FILE_NAME));

    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/shard_[0-9]+)?";
    std::string pkeyPattern = segmentPattern + "/index/\\w+/" + PREFIX_KEY_FILE_NAME;
    std::string skeyPattern = segmentPattern + "/index/\\w+/" + SUFFIX_KEY_FILE_NAME;
    std::string valuePattern = segmentPattern + "/index/\\w+/" + KKV_VALUE_FILE_NAME;
    ASSERT_EQ(pkeyPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_KKV_PKEY_"));
    ASSERT_EQ(skeyPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_KKV_SKEY_"));
    ASSERT_EQ(valuePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_KKV_VALUE_"));
}

} // namespace indexlib::index
