#include <string>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/Constant.h"
#include "unittest/unittest.h"

namespace indexlib::index {

class KVIndexFileTest : public TESTBASE
{
public:
    KVIndexFileTest() = default;
    ~KVIndexFileTest() = default;

public:
    void setUp() override {}
    void tearDown() override {}
};

TEST_F(KVIndexFileTest, TestLoadConfig)
{
    ASSERT_EQ("index", std::string(KV_INDEX_PATH));
    ASSERT_EQ("key", std::string(KV_KEY_FILE_NAME));
    ASSERT_EQ("value", std::string(KV_VALUE_FILE_NAME));

    std::string segmentPattern = std::string("^") + "(" + file_system::FILE_SYSTEM_ROOT_LINK_NAME +
                                 "(@[0-9]+)?/(__FENCE__)?" + RT_INDEX_PARTITION_DIR_NAME + "/)?" + "(" +
                                 PATCH_INDEX_DIR_PREFIX + "[0-9]+/)?" + "segment_[0-9]+(_level_[0-9]+)?" +
                                 "(/shard_[0-9]+)?";
    std::string kvKeyPattern = segmentPattern + "/index/\\w+/" + KV_KEY_FILE_NAME;
    std::string kvValuePattern = segmentPattern + "/index/\\w+/" + KV_VALUE_FILE_NAME;
    std::string kvValueCompressMetaPattern =
        segmentPattern + "/index/\\w+/" + KV_VALUE_FILE_NAME + file_system::COMPRESS_FILE_META_SUFFIX;
    ASSERT_EQ(kvKeyPattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_KV_KEY_"));
    ASSERT_EQ(kvValuePattern, file_system::LoadConfig::BuiltInPatternToRegexPattern("_KV_VALUE_"));
    ASSERT_EQ(kvValueCompressMetaPattern,
              file_system::LoadConfig::BuiltInPatternToRegexPattern("_KV_VALUE_COMPRESS_META_"));
}

} // namespace indexlib::index
