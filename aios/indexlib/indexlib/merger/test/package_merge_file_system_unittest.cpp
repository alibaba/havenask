#include "indexlib/merger/test/package_merge_file_system_unittest.h"
#include "indexlib/config/package_file_tag_config_list.h"
#include "indexlib/config/impl/merge_config_impl.h"

using namespace std;

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, PackageMergeFileSystemTest);

PackageMergeFileSystemTest::PackageMergeFileSystemTest()
{
}

PackageMergeFileSystemTest::~PackageMergeFileSystemTest()
{
}

void PackageMergeFileSystemTest::CaseSetUp()
{
}

void PackageMergeFileSystemTest::CaseTearDown()
{
}

void PackageMergeFileSystemTest::TestSimpleProcess()
{
}

void PackageMergeFileSystemTest::TestCheckpointManager()
{
    // typedef PackageMergeFileSystem::CheckpointManager CheckpointManager;
    // CheckpointManager cpm;
    // ASSERT_TRUE(FileSystemWrapper::IsExist());
    // ASSERT_THcpm.Recover();
}

void PackageMergeFileSystemTest::TestTagFunction()
{
    config::MergeConfig mergeConfig;
    auto TagFunction = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "");
    };
    ASSERT_EQ("", TagFunction("segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("", TagFunction("segment_203_level_0/summary/data"));

    mergeConfig.mImpl->packageFileTagConfigList.TEST_PATCH();
    auto TagFunction1 = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "MERGE");
    };
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/sub_segment/attribute/abc/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/attribute/pack_attr/sku_pidvid/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/attribute/test/test/sku_pidvid/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/index/title/posting"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/summary/data"));
    ASSERT_EQ("MERGE", TagFunction1("non-segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/attribute/sku_pidvid/1.patch"));

    string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" },
            {"file_patterns": ["_SUMMARY_DATA_"], "tag": "SUMMARY_DATA" },
            {"file_patterns": ["_SUMMARY_"], "tag": "SUMMARY" }
        ]
    }
    )";
    FromJsonString(mergeConfig.mImpl->packageFileTagConfigList, jsonStr);
    auto TagFunction2 = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "MERGE");
    };

    ASSERT_EQ("SUMMARY_DATA", TagFunction2("segment_203_level_0/summary/data"));
    ASSERT_EQ("SUMMARY", TagFunction2("segment_203_level_0/summary/offset"));
}


IE_NAMESPACE_END(merger);

