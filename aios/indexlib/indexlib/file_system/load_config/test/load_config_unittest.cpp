#include "indexlib/file_system/load_config/test/load_config_unittest.h"
#include "indexlib/file_system/load_config/mmap_load_strategy.h"
#include "indexlib/file_system/load_config/cache_load_strategy.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, LoadConfigTest);

LoadConfigTest::LoadConfigTest()
{
}

LoadConfigTest::~LoadConfigTest()
{
}

void LoadConfigTest::CaseSetUp()
{
}

void LoadConfigTest::CaseTearDown()
{
}

void LoadConfigTest::TestEqual()
{
    {
        LoadConfig left;
        LoadConfig right;
        INDEXLIB_TEST_TRUE(left.EqualWith(right));
    }
    {
        LoadConfig::FilePatternStringVector patterns;
        patterns.push_back(".*");
        LoadConfig left;
        left.SetFilePatternString(patterns);
        LoadConfig right;
        INDEXLIB_TEST_TRUE(!left.EqualWith(right));
    }
    {
        LoadStrategyPtr mmapLoadStrategy(new MmapLoadStrategy);
        LoadConfig left;
        left.SetLoadStrategyPtr(mmapLoadStrategy);
        LoadStrategyPtr cacheLoadStrategy(new CacheLoadStrategy);
        LoadConfig right;
        right.SetLoadStrategyPtr(cacheLoadStrategy);
        INDEXLIB_TEST_TRUE(!left.EqualWith(right));
    }
    {
        WarmupStrategy warmupStrategy;
        warmupStrategy.SetWarmupType(WarmupStrategy::WARMUP_SEQUENTIAL);
        LoadConfig left;
        left.SetWarmupStrategy(warmupStrategy);
        LoadConfig right;
        INDEXLIB_TEST_TRUE(!left.EqualWith(right));
    }
}

void LoadConfigTest::TestDefaultValue()
{
    string jsonStr = "\
    {                                                    \
        \"file_patterns\" : [\"_ATTRIBUTE_\"]            \
    }                                                    \
    ";
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);
    LoadConfig::FilePatternStringVector filePatterns = 
        loadConfig.GetFilePatternString();
    INDEXLIB_TEST_EQUAL((size_t)1, filePatterns.size());

    INDEXLIB_TEST_EQUAL(READ_MODE_MMAP, loadConfig.GetLoadStrategyName());
    INDEXLIB_TEST_EQUAL(WarmupStrategy::WARMUP_NONE,
                        loadConfig.GetWarmupStrategy().GetWarmupType());
}

void LoadConfigTest::TestRegularExpressionParse()
{
    string jsonStr = "\
    {                                                    \
        \"file_patterns\" : [\".*\"]                     \
    }                                                    \
";
    LoadConfig loadConfig;
    FromJsonString(loadConfig, jsonStr);
    RegularExpressionVector regexVector = loadConfig.GetFilePatternRegex();
    INDEXLIB_TEST_EQUAL((size_t)1, regexVector.size());

}

bool LoadConfigTest::DoTestMatch(const string& pattern, const string& path)
{
    LoadConfig loadConfig;
    LoadConfig::FilePatternStringVector filePatterns;
    filePatterns.push_back(pattern);
    loadConfig.SetFilePatternString(filePatterns);
    return loadConfig.Match(path, "");
}

void LoadConfigTest::TestMatch()
{
    ASSERT_TRUE(DoTestMatch(".*", "xxxx"));
}

void LoadConfigTest::TestMatchBuiltInPattern()
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
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/index/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/index/a/b/c"));

    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_1234567890/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_10/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/attribute/a/b/c"));

    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_1234567890/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_10/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_10/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10/summary/a/b/c"));
}

void LoadConfigTest::TestMatchBuiltInPatternWithLevel()
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
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/index/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10_level_0/index/a/b/c"));


    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_1234567890_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_10_level_0/attribute/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/attribute/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/attribute/pack_attr/sku/2_1.patch"));
    
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_0/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_DATA_", "segment_0_level_0/summary/data"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_0/index/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_1234567890_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_10_level_0/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_1234567890_level_0/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_10_level_0/summary/a/b/c"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@201/rt_index_partition/segment_10_level_0/summary/a/b/c"));

}


void LoadConfigTest::TestMatchBuiltInPatternWithSub()
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
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0/sub_segment/summary/a"));


    // 2 sub_segment
    ASSERT_FALSE(DoTestMatch("_INDEX_",
                             "segment_0/sub_segment/sub_segment/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_",
                             "segment_0/sub_segment/sub_segment/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_",
                             "segment_0/sub_segment/sub_segment/summary/a"));
}

void LoadConfigTest::TestMatchBuiltInPatternWithSubWithLevel()
{
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0/sub_segment/index"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/sub_segment/attribute"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_", "segment_0_level_32423/sub_segment/summary"));
    ASSERT_FALSE(DoTestMatch("_INDEX_", "segment_0_level_0(/sub_segment)/index/a"));

    // 1 sub_segment:
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/attribute/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_PATCH_", "segment_3_level_0/sub_segment/attribute/pack_attr/sku/2_1.patch"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "segment_0_level_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_DATA_", "segment_0_level_0/sub_segment/summary/data"));

    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__/rt_index_partition/segment_0_level_0/sub_segment/summary/a"));
    ASSERT_TRUE(DoTestMatch("_INDEX_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/index/a"));
    ASSERT_TRUE(DoTestMatch("_ATTRIBUTE_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/attribute/a"));
    ASSERT_TRUE(DoTestMatch("_SUMMARY_", "__indexlib_fs_root_link__@101/rt_index_partition/segment_0_level_0/sub_segment/summary/a"));


    // 2 sub_segment
    ASSERT_FALSE(DoTestMatch("_INDEX_",
                             "segment_0_level_3453/sub_segment/sub_segment/index/a"));
    ASSERT_FALSE(DoTestMatch("_ATTRIBUTE_",
                             "segment_0_level_0/sub_segment/sub_segment/attribute/a"));
    ASSERT_FALSE(DoTestMatch("_SUMMARY_",
                             "segment_0_level_0/sub_segment/sub_segment/summary/a"));
}

void LoadConfigTest::TestBuiltInPatternToRegexPattern()
{
    INDEXLIB_TEST_EQUAL(string("no_built_in_pattern"),
                        LoadConfig::BuiltInPatternToRegexPattern("no_built_in_pattern"));
    INDEXLIB_TEST_EQUAL(string("^(__indexlib_fs_root_link__(@[0-9]+)?/rt_index_partition/)?(patch_index_[0-9]+/)?segment_[0-9]+(_level_[0-9]+)?(/sub_segment)?/attribute/"),
                        LoadConfig::BuiltInPatternToRegexPattern("_ATTRIBUTE_"));
}

IE_NAMESPACE_END(file_system);

