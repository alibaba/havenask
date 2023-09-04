#include "indexlib/merger/test/merge_partition_data_creator_unittest.h"

#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/merger/merge_partition_data.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace indexlib::test;
using namespace indexlib::config;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergePartitionDataCreatorTest);

MergePartitionDataCreatorTest::MergePartitionDataCreatorTest() {}

MergePartitionDataCreatorTest::~MergePartitionDataCreatorTest() {}

void MergePartitionDataCreatorTest::CaseSetUp()
{
    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mOptions.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;
    mOptions.GetMergeConfig().mImpl->enablePackageFile = true;

    string field = "key:int32;value:uint64;";
    mSchema = SchemaMaker::MakeKVSchema(field, "key", "value");
}

void MergePartitionDataCreatorTest::CaseTearDown() {}

void MergePartitionDataCreatorTest::TestCreateMergePartitionData()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    string part1 = GET_TEMP_DATA_PATH() + "/partition_0_32767";
    string part2 = GET_TEMP_DATA_PATH() + "/partition_32768_65535";
    {
        string docString = "cmd=add,key=1,value=1,ts=10000000;"
                           "cmd=add,key=2,value=2,ts=30000000";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, part1));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    {
        string docString = "cmd=add,key=1,value=7,ts=10000001;"
                           "cmd=add,key=3,value=3,ts=30000000";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, part2));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    RESET_FILE_SYSTEM("", false, FileSystemOptions::Offline());
    {
        vector<file_system::DirectoryPtr> roots;
        roots.push_back(GET_PARTITION_DIRECTORY()->GetDirectory("partition_0_32767", true));
        roots.push_back(GET_PARTITION_DIRECTORY()->GetDirectory("partition_32768_65535", true));
        vector<Version> versions;
        Version v1, v2;
        VersionLoader::GetVersion(Directory::GetPhysicalDirectory(part1), v1, 0);
        VersionLoader::GetVersion(Directory::GetPhysicalDirectory(part2), v2, 0);
        versions.push_back(v1);
        versions.push_back(v2);
        config::IndexPartitionOptions options;
        MergePartitionDataPtr partData =
            MergePartitionDataCreator::CreateMergePartitionData(roots, versions, false, PluginManagerPtr());
        ASSERT_TRUE(partData);
        MultiPartSegmentDirectoryPtr multiDir =
            DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, partData->mSegmentDirectory);
        ASSERT_TRUE(multiDir);
        for (auto segDir : multiDir->mSegmentDirectoryVec) {
            ASSERT_TRUE(segDir->mRootDirectory);
            IFileSystemPtr fs = DYNAMIC_POINTER_CAST(IFileSystem, segDir->mRootDirectory->GetFileSystem());
            ASSERT_TRUE(fs);
        }
    }
}
}} // namespace indexlib::merger
