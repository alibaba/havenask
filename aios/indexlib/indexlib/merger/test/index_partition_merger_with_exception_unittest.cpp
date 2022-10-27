#include <fslib/fs/FileSystem.h>
#include <autil/StringUtil.h>
#include "indexlib/merger/test/index_partition_merger_with_exception_unittest.h"
#include "indexlib/merger/partition_merger_creator.h"
#include "indexlib/merger/multi_partition_merger.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/merger/merge_work_item_creator.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/util/path_util.h"

using namespace std;
using namespace fslib;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(merger);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index_base);

DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, IndexPartitionMergerWithExceptionTest);

namespace
{
class MockMergeWorkItemCreator : public MergeWorkItemCreator
{
public:
    MockMergeWorkItemCreator(const config::IndexPartitionSchemaPtr& schema,
        const config::MergeConfig& mergeConfig, const IndexMergeMeta* mergeMeta,
        const merger::SegmentDirectoryPtr& segmentDirectory,
        const merger::SegmentDirectoryPtr& subSegmentDirectory, bool isSortedMerge, bool optimize,
        const std::string& dumpPath, const config::IndexPartitionOptions& options)
        : MergeWorkItemCreator(schema, mergeConfig, mergeMeta, segmentDirectory,
            subSegmentDirectory, isSortedMerge, optimize, NULL, util::CounterMapPtr(), options,
            MergeFileSystem::Create(dumpPath, mergeConfig, 0, storage::RaidConfigPtr()),
            plugin::PluginManagerPtr())
    {
    }

    MOCK_METHOD1(CreateMergeWorkItem,
                 MergeWorkItem* (const MergeTaskItem &item));
};

class MockMergeFileSystem : public MergeFileSystem
{
public:
    MockMergeFileSystem() : MergeFileSystem("", config::MergeConfig(), 0, storage::RaidConfigPtr()) {}
    void MakeCheckpoint(const std::string& fileName) override {}
    bool HasCheckpoint(const std::string& fileName) override { return false; }
};

class MockMergeWorkItem : public MergeWorkItem
{
public:
    MockMergeWorkItem() : MergeWorkItem(MergeFileSystemPtr(new MockMergeFileSystem())) {}
    ~MockMergeWorkItem() = default;

    MOCK_METHOD0(DoProcess, void());
    int64_t EstimateMemoryUse() const { return 0; }
    std::string GetIdentifier() const { return ""; }
    void Destroy() {}
    int64_t GetRequiredResource() const { return 0; }
};

class MockIndexPartitionMerger : public IndexPartitionMerger
{
public:
    MockIndexPartitionMerger(const SegmentDirectoryPtr& segDir, 
                             const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options)
        : IndexPartitionMerger(segDir, schema, options, merger::DumpStrategyPtr(),
                               NULL, plugin::PluginManagerPtr())
    {}
    MOCK_METHOD3(CreateMergeWorkItemCreator,
                 MergeWorkItemCreatorPtr(
                         bool, const merger::IndexMergeMeta&, const MergeFileSystemPtr&));
};
}

IndexPartitionMergerWithExceptionTest::IndexPartitionMergerWithExceptionTest()
{
}

IndexPartitionMergerWithExceptionTest::~IndexPartitionMergerWithExceptionTest()
{
}

void IndexPartitionMergerWithExceptionTest::CaseSetUp()
{
    string field = "pk:string:pk;long1:uint32;string1:string";
    string index = "pk:primarykey64:pk;long1:number:long1;string1:string:string1;";
    string attr = "long1;pk";
    string summary = "long1;";
    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);

    mRootDir = GET_TEST_DATA_PATH();
}

void IndexPartitionMergerWithExceptionTest::CaseTearDown()
{
}

void IndexPartitionMergerWithExceptionTest::TestMergeParallelWithException()
{
    SingleFieldPartitionDataProvider provider;
    string rootDir = GET_TEST_DATA_PATH();
    provider.Init(rootDir, "int32", SFP_PK_INDEX);
    provider.Build("1#2#3#4", SFP_OFFLINE);
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    SegmentDirectoryPtr segDir(new SegmentDirectory(rootDir, version));
    segDir->Init(false, true);

    IndexPartitionOptions options = provider.GetOptions();
    MergeConfig& mergeConfig = options.GetMergeConfig();
    mergeConfig.mergeThreadCount = 2;
    MockIndexPartitionMerger* mockMerger = 
        new MockIndexPartitionMerger(segDir, provider.GetSchema(), options);
    IndexPartitionMergerPtr merger(mockMerger);
    IndexMergeMeta mergeMeta;
    MockMergeWorkItemCreator* mockCreator = new MockMergeWorkItemCreator(
            provider.GetSchema(), mergeConfig, &mergeMeta,
            segDir, segDir, false, true, "instance_0", options);
    MergeWorkItemCreatorPtr creator(mockCreator);

    MockMergeWorkItem* mockWorkItem1 = new MockMergeWorkItem();
    MockMergeWorkItem* mockWorkItem2 = new MockMergeWorkItem();

    EXPECT_CALL(*mockMerger, CreateMergeWorkItemCreator(_,_,_))
        .WillOnce(Return(creator));
    EXPECT_CALL(*mockCreator, CreateMergeWorkItem(_))
        .WillOnce(Return(mockWorkItem1))
        .WillOnce(Return(mockWorkItem2));
    EXPECT_CALL(*mockWorkItem1, DoProcess())
        .WillOnce(Return());
    EXPECT_CALL(*mockWorkItem2, DoProcess())
        .WillOnce(Throw(FileIOException()));
    ASSERT_THROW(merger->Merge(true), misc::ExceptionBase);
}

void IndexPartitionMergerWithExceptionTest::TestEndMergeException()
{
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    string docString = "cmd=add,pk=1,long1=1,string1=1;"
                       "cmd=add,pk=2,long1=2,string1=2;"
                       "cmd=add,pk=3,long1=3,string1=3;"
                       "cmd=add,pk=4,long1=2,string1=4;";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docString,
                             "long1:2", "docid=1;docid=3"));

    IndexPartitionMergerPtr partMerger(dynamic_cast<IndexPartitionMerger*>(
                    PartitionMergerCreator::CreateSinglePartitionMerger(
                            mRootDir, mOptions, NULL, "")));
    MergeMetaPtr mergeMeta = partMerger->CreateMergeMeta(false, 2, 0);
    partMerger->DoMerge(false, mergeMeta, 0);
    partMerger->DoMerge(false, mergeMeta, 1);
    if (!mOptions.GetMergeConfig().GetEnablePackageFile())
    {
        string instancePkIndexPath = FileSystemWrapper::JoinPath(
            mRootDir, "instance_0/segment_1_level_0/index/pk");
        string instanceString1IndexPath = FileSystemWrapper::JoinPath(
            mRootDir, "instance_0/segment_1_level_0/index/long1");
        ASSERT_TRUE(FileSystemWrapper::IsDir(instancePkIndexPath));
        ASSERT_TRUE(FileSystemWrapper::IsDir(instanceString1IndexPath));

        FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR, instancePkIndexPath);
        FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR, instanceString1IndexPath);
    }
    else
    {
        string segmentPath = PathUtil::JoinPath(mRootDir, "instance_0/segment_1_level_0");
        FileList fileList;
        FileSystemWrapper::ListDir(segmentPath, fileList, false);
        size_t setErrorTimes = 0;
        string packageDataFilePrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_DATA_SUFFIX;
        for (const string& file : fileList)
        {
            if (setErrorTimes < 2 && StringUtil::startsWith(file, packageDataFilePrefix))
            {
                FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR,
                                            PathUtil::JoinPath(segmentPath, file));
                setErrorTimes += 1;
            }
        }
        ASSERT_TRUE(setErrorTimes == 2);
    }
    ASSERT_THROW(partMerger->EndMerge(mergeMeta), misc::FileIOException);
    {
        // retry 1 : throw
        IndexPartitionMergerPtr partMerger1(dynamic_cast<IndexPartitionMerger*>(
                        PartitionMergerCreator::CreateSinglePartitionMerger(
                                mRootDir, mOptions, NULL, "")));
        ASSERT_THROW(partMerger1->EndMerge(mergeMeta), misc::FileIOException); 
    }
    {
        // retry 2 : no throw
        IndexPartitionMergerPtr partMerger2(dynamic_cast<IndexPartitionMerger*>(
                        PartitionMergerCreator::CreateSinglePartitionMerger(
                                mRootDir, mOptions, NULL, "")));
        ASSERT_NO_THROW(partMerger->EndMerge(mergeMeta));
    }
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "pk:2", "pk=2"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "string1:2", "pk=2"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:3", "pk=3"));
}

void IndexPartitionMergerWithExceptionTest::TestMultiPartitionEndMergeException()
{
    string partOneDocStr = "cmd=add,pk=1,long1=1,string1=1;"
                           "cmd=add,pk=2,long1=2,string1=2;";
    string partTwoDocStr = "cmd=add,pk=3,long1=3,string1=3;"
                           "cmd=add,pk=4,long1=2,string1=4;";
    // prepare partitions
    vector<string> docStrs;
    docStrs.push_back(partOneDocStr);
    docStrs.push_back(partTwoDocStr);
    vector<string> partPaths;
    partPaths.push_back("part1");
    partPaths.push_back("part2");
    assert(docStrs.size() == partPaths.size());
    for (size_t i = 0; i < docStrs.size(); ++i)
    {
        GET_PARTITION_DIRECTORY()->MakeDirectory(partPaths[i]);
        string rootDir = GET_TEST_DATA_PATH() + partPaths[i];
        PartitionStateMachine psm;
        psm.Init(mSchema, mOptions, rootDir);
        ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, docStrs[i], "", ""));
    }
    // multi partition merge
    vector<string> mergeSrcs;
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part1");
    mergeSrcs.push_back(GET_TEST_DATA_PATH() + "part2");

    GET_PARTITION_DIRECTORY()->MakeDirectory("mergePart");
    string mergePartPath = GET_TEST_DATA_PATH() + "mergePart";
    mOptions.SetIsOnline(false);

    MultiPartitionMerger multiPartMerger(mOptions, NULL, "");
    IndexPartitionMergerPtr partMerger = multiPartMerger.CreatePartitionMerger(mergeSrcs, mergePartPath);

    MergeMetaPtr mergeMeta = partMerger->CreateMergeMeta(false, 2, 0);
    partMerger->DoMerge(false, mergeMeta, 0);
    partMerger->DoMerge(false, mergeMeta, 1);
    if (!mOptions.GetMergeConfig().GetEnablePackageFile())
    {
        string instancePkIndexPath = FileSystemWrapper::JoinPath(
            mergePartPath, "instance_0/segment_0_level_0/index/pk");
        string instanceString1IndexPath = FileSystemWrapper::JoinPath(
            mergePartPath, "instance_0/segment_0_level_0/index/long1");
        ASSERT_TRUE(FileSystemWrapper::IsDir(instancePkIndexPath));
        ASSERT_TRUE(FileSystemWrapper::IsDir(instanceString1IndexPath));

        FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR, instancePkIndexPath);
        FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR, instanceString1IndexPath);
    }
    else
    {
        string segmentPath = PathUtil::JoinPath(mergePartPath, "instance_0/segment_0_level_0");
        FileList fileList;
        FileSystemWrapper::ListDir(segmentPath, fileList, false);
        size_t setErrorTimes = 0;
        string packageDataFilePrefix = string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_DATA_SUFFIX;
        for (const string& file : fileList)
        {
            if (setErrorTimes < 2 && StringUtil::startsWith(file, packageDataFilePrefix))
            {
                FileSystemWrapper::SetError(FileSystemWrapper::RENAME_ERROR,
                                            PathUtil::JoinPath(segmentPath, file));
                setErrorTimes += 1;
            }
        }
        ASSERT_TRUE(setErrorTimes == 2);
    }
    ASSERT_THROW(partMerger->EndMerge(mergeMeta), misc::FileIOException);
    {
        // retry 1 : throw
        IndexPartitionMergerPtr partMerger1 = multiPartMerger.CreatePartitionMerger(mergeSrcs, mergePartPath);
        ASSERT_THROW(partMerger1->EndMerge(mergeMeta),
                     misc::FileIOException);
    }
    {
        // retry 2 : no throw
        IndexPartitionMergerPtr partMerger2 = multiPartMerger.CreatePartitionMerger(mergeSrcs, mergePartPath);
        ASSERT_NO_THROW(partMerger->EndMerge(mergeMeta));
    }
    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mergePartPath));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "pk:2", "pk=2"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "string1:2", "pk=2"));
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "long1:3", "pk=3"));
}

IE_NAMESPACE_END(merger);

