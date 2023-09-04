#include "indexlib/file_system/load_config/LoadConfig.h"

#include "gtest/gtest.h"
#include <cstddef>

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/file_system/load_config/MmapLoadStrategy.h"
#include "indexlib/file_system/load_config/WarmupStrategy.h"
#include "indexlib/util/RegularExpression.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {
class LoadConfigTest : public TESTBASE
{
private:
    bool DoTestMatch(const std::string& pattern, const std::string& path);
};

TEST_F(LoadConfigTest, TestEqual)
{
    {
        LoadConfig left;
        LoadConfig right;
        ASSERT_TRUE(left.EqualWith(right));
    }
    {
        LoadConfig::FilePatternStringVector patterns;
        patterns.push_back(".*");
        LoadConfig left;
        left.SetFilePatternString(patterns);
        LoadConfig right;
        ASSERT_TRUE(!left.EqualWith(right));
    }
    {
        LoadStrategyPtr mmapLoadStrategy(new MmapLoadStrategy);
        LoadConfig left;
        left.SetLoadStrategyPtr(mmapLoadStrategy);
        LoadStrategyPtr cacheLoadStrategy(new CacheLoadStrategy);
        LoadConfig right;
        right.SetLoadStrategyPtr(cacheLoadStrategy);
        ASSERT_TRUE(!left.EqualWith(right));
    }
    {
        WarmupStrategy warmupStrategy;
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_SEQUENTIAL);
        LoadConfig left;
        left.SetWarmupStrategy(warmupStrategy);
        LoadConfig right;
        ASSERT_TRUE(!left.EqualWith(right));
    }
}

TEST_F(LoadConfigTest, TestDefaultValue)
{
    string jsonStr = "\
    {                                                    \
        \"file_patterns\" : [\"_ATTRIBUTE_\"]            \
    }                                                    \
    ";
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);
    LoadConfig::FilePatternStringVector filePatterns = loadConfig.GetFilePatternString();
    ASSERT_EQ((size_t)1, filePatterns.size());

    ASSERT_EQ(READ_MODE_MMAP, loadConfig.GetLoadStrategyName());
    ASSERT_EQ(WarmupStrategy::WARMUP_NONE, loadConfig.GetWarmupStrategy().GetWarmupType());
}

TEST_F(LoadConfigTest, TestRegularExpressionParse)
{
    string jsonStr = "\
    {                                                    \
        \"file_patterns\" : [\".*\"]                     \
    }                                                    \
";
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);
    util::RegularExpressionVector regexVector = loadConfig.GetFilePatternRegex();
    ASSERT_EQ((size_t)1, regexVector.size());
}

bool LoadConfigTest::DoTestMatch(const string& pattern, const string& path)
{
    LoadConfig loadConfig;
    LoadConfig::FilePatternStringVector filePatterns;
    filePatterns.push_back(pattern);
    loadConfig.SetFilePatternString(filePatterns);
    return loadConfig.Match(path, "");
}

TEST_F(LoadConfigTest, TestMatch) { ASSERT_TRUE(DoTestMatch(".*", "xxxx")); }

TEST_F(LoadConfigTest, TestMatchBuiltInPattern)
{
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0/index"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "/segment_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "Xsegment_0/index/a"));

    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0/summary/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_1234567890/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_10/index/a/b/c"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "rt_index_partition/segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/index/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/index/a/b/c"));

    ASSERT_TRUE(DoTestMatch("_INDEX_DICTIONARY_", "segment_0/index/a/dictionary"));
    ASSERT_TRUE(DoTestMatch("_INDEX_POSTING_", "segment_0/index/a/posting"));
    ASSERT_TRUE(DoTestMatch("_PK_ATTRIBUTE_", "segment_0/index/a/attribute_a"));
    ASSERT_FALSE(DoTestMatch("_SECTION_ATTRIBUTE_", "segment_0/index/attribute_a"));
    ASSERT_TRUE(DoTestMatch("_SECTION_ATTRIBUTE_", "segment_0/index/aaa_section/"));

    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_1234567890/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_10/attribute/a/b/c"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "rt_index_partition/segment_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/attribute/a"));
    ASSERT_TRUE(
        DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/attribute/a/b/c"));
    ASSERT_TRUE(
        DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/attribute/a/b/c"));

    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_DATA_", "segment_0/attribute/foo/data"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_DATA_", "segment_0/attribute/foo/aaa"));

    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_OFFSET_", "segment_0/attribute/foo/offset"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_OFFSET_", "segment_0/attribute/offset/aaa"));

    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_1234567890/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_10/summary/a/b/c"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "rt_index_partition/segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/summary/a/b/c"));
    ASSERT_FALSE(DoTestMatch("_SOURCE_", "segment_0/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SOURCE_", "segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "segment_0/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "segment_1234567890/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "segment_10/source/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/source/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/source/a/b/c"));
}

TEST_F(LoadConfigTest, TestMatchBuiltInPatternWithLevel)
{
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0/index"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "/segment_0_level_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "Xsegment_0_level_0/index/a"));

    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_d/index/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_/index/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "level_9/index/a"));

    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0/summary/a"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_0_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_1234567890_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_10_level_0/index/a/b/c"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "rt_index_partition/segment_0_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/index/a"));
    ASSERT_TRUE(
        DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/index/a/b/c"));
    ASSERT_TRUE(
        DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10_level_0/index/a/b/c"));

    ASSERT_TRUE(DoTestMatch("_INDEX_DICTIONARY_", "segment_0_level_0/index/strIndex/dictionary"));
    ASSERT_TRUE(DoTestMatch("_INDEX_POSTING_", "segment_0_level_0/index/strIndex/posting"));
    ASSERT_TRUE(DoTestMatch("_INDEX_POSTING_", "segment_0_level_0/index/strIndex/posting"));
    ASSERT_TRUE(DoTestMatch("_PK_ATTRIBUTE_", "segment_0_level_0/index/pk/attribute_string1/data"));

    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_1234567890_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(
        DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_",
                            "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/attribute/a"));
    ASSERT_TRUE(
        DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_",
                            "__indexlib_fs_root_link__@101/rt_index_partition/segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/attribute/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/attribute/pack_attr/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_0/attribute/price/201_175.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/index/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_0/index/price/201_175.patch"));
    ASSERT_FALSE(DoTestMatch("_PATCH_", "segment_3_level_0/i/sku/2_1.patch"));

    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_DATA_", "segment_0_level_0/summary/data"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_1234567890_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_10_level_0/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/summary/a"));
    ASSERT_TRUE(
        DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/summary/a"));
    ASSERT_TRUE(
        DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/summary/a/b/c"));
    ASSERT_TRUE(
        DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@201/rt_index_partition/segment_10_level_0/summary/a/b/c"));
}

TEST_F(LoadConfigTest, TestMatchBuiltInPatternWithSub)
{
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0/sub_segment/index"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/sub_segment/attribute"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/sub_segment/summary"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0(/sub_segment)/index/a"));

    // 1 sub_segment:
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/sub_segment/index/a"));
    ASSERT_TRUE(
        DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/sub_segment/attribute/a"));
    ASSERT_TRUE(
        DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/sub_segment/summary/a"));
    ASSERT_TRUE(
        DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_",
                            "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/attribute/a"));
    ASSERT_TRUE(
        DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/summary/a"));

    // 2 sub_segment
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0/sub_segment/sub_segment/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/sub_segment/sub_segment/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/sub_segment/sub_segment/summary/a"));
}

TEST_F(LoadConfigTest, TestMatchBuiltInPatternWithSubWithLevel)
{
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0/sub_segment/index"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/sub_segment/attribute"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_32423/sub_segment/summary"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0(/sub_segment)/index/a"));

    // 1 sub_segment:
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/attribute/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/attribute/pack_attr/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/index/sku/2_1.patch"));
    ASSERT_FALSE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/attr/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SECTION_ATTRIBUTE_", "segment_0_level_0/sub_segment/index/aaa_section/data"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0_level_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_DATA_", "segment_0_level_0/sub_segment/summary/data"));

    ASSERT_TRUE(DoTestMatch("_SOURCE_", "segment_0_level_0/sub_segment/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_DATA_", "segment_0_level_0/sub_segment/source/group_10/data"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_META_", "segment_0_level_0/sub_segment/source/meta/data"));

    ASSERT_TRUE(DoTestMatch("_SOURCE_", "segment_0_level_0/sub_segment/source/a"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_DATA_", "segment_0_level_0/sub_segment/source/group_1/data"));
    ASSERT_FALSE(DoTestMatch("_SOURCE_DATA_", "segment_0_level_0/sub_segment/source/meta/data"));
    ASSERT_TRUE(DoTestMatch("_SOURCE_META_", "segment_0_level_0/sub_segment/source/meta/data"));
    ASSERT_FALSE(DoTestMatch("_SOURCE_META_", "segment_0_level_0/sub_segment/source/meta/data_2"));

    ASSERT_TRUE(
        DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_",
                            "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_",
                            "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_",
                            "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch(
        "_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch(
        "_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/summary/a"));

    // 2 sub_segment
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_3453/sub_segment/sub_segment/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/sub_segment/sub_segment/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SECTION_ATTRIBUTE_", "segment_0_level_0/sub_segment/sub_segment/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_0/sub_segment/sub_segment/summary/a"));
}

TEST_F(LoadConfigTest, TestBuiltInPatternToRegexPattern)
{
    ASSERT_EQ(string("no_built_in_pattern"), LoadConfig::BuiltInPatternToRegexPattern("no_built_in_pattern"));
    ASSERT_EQ(string("^(__indexlib_fs_root_link__(@[0-9]+)?/(__FENCE__)?rt_index_partition/)?(patch_index_[0-9]+/"
                     ")?segment_[0-9]+(_level_[0-9]+)?(/sub_segment)?/attribute/"),
              LoadConfig::BuiltInPatternToRegexPattern("_ATTRIBUTE_"));
}

TEST_F(LoadConfigTest, TestMatchBuiltInPatternForKV)
{
    ASSERT_FALSE(DoTestMatch("_KV_KEY_",
                             "__FENCE__rt_index_partition/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_FALSE(
        DoTestMatch("_KV_KEY_", "rt_index_partition/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_FALSE(DoTestMatch("_KV_KEY_", "__FENCE__1/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_TRUE(DoTestMatch("_KV_KEY_", "patch_index_2/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_FALSE(DoTestMatch("_KV_KEY_",
                             "patch_index_2/patch_index_2/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_FALSE(DoTestMatch("_KV_KEY_", "^segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_TRUE(DoTestMatch(
        "_KV_KEY_",
        "__indexlib_fs_root_link__@1/rt_index_partition/segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_TRUE(DoTestMatch("_KV_KEY_", "segment_536875565_level_0/shard_1/index/biz_order_id/key"));
    ASSERT_TRUE(DoTestMatch("_KV_VALUE_", "segment_536875565_level_0/shard_1/index/biz_order_id/value"));
    ASSERT_TRUE(DoTestMatch("_KV_KEY_", "segment_0_level_1/index/biz_order_id/key"));
    ASSERT_TRUE(DoTestMatch("_KV_VALUE_", "segment_0_level_1/index/biz_order_id/value"));
    ASSERT_TRUE(DoTestMatch("_KV_VALUE_", "segment_0_level_1/index/biz_order_id/value.__compress_info__"));

    ASSERT_FALSE(DoTestMatch("_KV_KEY_", "segment_536875565_level_0/shard_1/index/biz_order_id/value"));
    ASSERT_FALSE(DoTestMatch("_KV_VALUE_", "segment_536875565_level_0/shard_1/index/biz_order_id/key"));

    ASSERT_TRUE(DoTestMatch("_KKV_PKEY_", "segment_0_level_2/index/trigger_id_item_id/pkey"));
    ASSERT_TRUE(DoTestMatch("_KKV_SKEY_", "segment_0_level_2/index/trigger_id_item_id/skey"));
    ASSERT_TRUE(DoTestMatch("_KKV_VALUE_", "segment_0_level_2/index/trigger_id_item_id/value"));
}

}} // namespace indexlib::file_system
