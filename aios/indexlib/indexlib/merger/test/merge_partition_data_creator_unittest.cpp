#include "indexlib/merger/test/merge_partition_data_creator_unittest.h"
#include "indexlib/merger/merge_partition_data_creator.h"
#include "indexlib/merger/merge_partition_data.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/multi_part_segment_directory.h"
#include "indexlib/file_system/indexlib_file_system_impl.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(plugin);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MergePartitionDataCreatorTest);

MergePartitionDataCreatorTest::MergePartitionDataCreatorTest()
{
}

MergePartitionDataCreatorTest::~MergePartitionDataCreatorTest()
{
}

void MergePartitionDataCreatorTest::CaseSetUp()
{
    mOptions = IndexPartitionOptions();
    mOptions.GetBuildConfig(true).buildTotalMemory = 22;
    mOptions.GetBuildConfig(false).buildTotalMemory = 40;
    mOptions.GetBuildConfig(false).levelTopology = topo_hash_mod;
    mOptions.GetBuildConfig(false).levelNum = 2;
    mOptions.GetBuildConfig(false).keepVersionCount = 100;
    mOptions.GetMergeConfig().mImpl->enablePackageFile = true;

    mSchema = SchemaMaker::MakeSchema(
            //Field schema
            "text1:text;string1:string",
            //Index schema
            "pack2:pack:text1;"
            //Primary key index schema
            "pk:primarykey64:string1",
            //Attribute schema
            "string1",
            //Summary schema
            "string1;text1" );
}

void MergePartitionDataCreatorTest::CaseTearDown()
{
}

void MergePartitionDataCreatorTest::TestCreateMergePartitionData()
{
    mOptions.GetBuildConfig(true).maxDocCount = 1;
    mOptions.GetBuildConfig(false).maxDocCount = 1;
    string part1 = GET_TEST_DATA_PATH() + "/partition_0_32767";
    string part2 = GET_TEST_DATA_PATH() + "/partition_32768_65535";
    {
        string docString = "cmd=add,string1=1,text1=text1;"
                           "cmd=add,string1=2,text1=text2;";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, part1));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    {
        string docString = "cmd=add,string1=1,text1=text1;"
                           "cmd=add,string1=3,text1=text3;";
        PartitionStateMachine psm;
        ASSERT_TRUE(psm.Init(mSchema, mOptions, part2));
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString, "", ""));
    }
    {
        vector<string> roots;
        roots.push_back(part1);
        roots.push_back(part2);
        vector<Version> versions;
        Version v1, v2;
        VersionLoader::GetVersion(part1, v1, 0);
        VersionLoader::GetVersion(part2, v2, 0);
        versions.push_back(v1);
        versions.push_back(v2);
        config::IndexPartitionOptions options;
        MergePartitionDataPtr partData = MergePartitionDataCreator::CreateMergePartitionData(
            roots, versions, false, PluginManagerPtr());
        ASSERT_TRUE(partData);
        MultiPartSegmentDirectoryPtr multiDir = DYNAMIC_POINTER_CAST(MultiPartSegmentDirectory, partData->mSegmentDirectory);
        ASSERT_TRUE(multiDir);
        for (auto segDir : multiDir->mSegmentDirectoryVec)
        {
            ASSERT_TRUE(segDir->mRootDirectory);
            IndexlibFileSystemImplPtr fs = DYNAMIC_POINTER_CAST(IndexlibFileSystemImpl, segDir->mRootDirectory->mFileSystem);
            ASSERT_TRUE(fs);
            ASSERT_TRUE(fs->mOptions.enablePathMetaContainer);
        }
    }
}

IE_NAMESPACE_END(merger);
