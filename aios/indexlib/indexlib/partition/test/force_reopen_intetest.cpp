#include "indexlib/partition/test/force_reopen_intetest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, ForceReopenInteTest);

ForceReopenInteTest::ForceReopenInteTest()
{
    mFlushRtIndex = (time(NULL) % 2 == 0);
}

ForceReopenInteTest::~ForceReopenInteTest()
{
}

void ForceReopenInteTest::CaseSetUp()
{
    mRootPath = GET_TEST_DATA_PATH();
    string field = "pk:string:pk;string1:string;text1:text;"
                   "long1:uint32;multi_long:uint32:true;"
                   "updatable_multi_long:uint32:true:true;";
    string index = "pk:primarykey64:pk;index1:string:string1;pack1:pack:text1;";
    string attr = "long1;multi_long;updatable_multi_long;";
    string summary = "string1;";

    mSchema = SchemaMaker::MakeSchema(field, index, 
            attr, summary);
    mOptions = IndexPartitionOptions();
    mOptions.GetOnlineConfig().SetMaxReopenMemoryUse(0);
    mBuildIncEvent = BUILD_INC_NO_MERGE_FORCE_REOPEN;
}

void ForceReopenInteTest::CaseTearDown()
{
}

void ForceReopenInteTest::TestForceReopen()
{
    SCOPED_TRACE("Failed");
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    string incDocString = "cmd=add,pk=1,string1=hello";
    ASSERT_TRUE(!psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                             "index1:hello", "docid=0"));
}

void ForceReopenInteTest::TestForceReopenWithCleanRtSegments()
{
    IndexPartitionOptions options;
    options.GetOnlineConfig().SetMaxReopenMemoryUse(0);
    options.GetBuildConfig().maxDocCount = 1;

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));

    string rtDocString = "cmd=add,pk=1,string1=hello,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    string incDocString = "cmd=add,pk=1,string1=hello,ts=2";
    ASSERT_TRUE(!psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                             "index1:hello", "docid=0"));
}

void ForceReopenInteTest::TestForceReopenWithCleanJoinSegments()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
    string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    string rtDocString = "cmd=update_field,pk=1,long1=4,ts=4";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));

    string incDocString = "cmd=update_field,pk=1,long1=2,ts=2";
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                             "index1:hello", "docid=0,long1=4"));
}

void ForceReopenInteTest::TestForceReopenFailed() 
{
    SCOPED_TRACE("Failed");
    PartitionStateMachine psm(8);
    mOptions.GetOnlineConfig().SetMaxReopenMemoryUse(OnlineConfig::INVALID_MAX_REOPEN_MEMORY_USE);

    string jsonStr = "                                                \
    {                                                                   \
    \"load_config\" :                                                   \
    [                                                                   \
    {                                                                   \
        \"file_patterns\" : [\".*\"],                                   \
        \"load_strategy\" : \"mmap\",                                   \
        \"load_strategy_param\" : {                                     \
            \"lock\" : true                                             \
        }                                                               \
    }                                                                   \
    ]                                                                   \
    }                                                                   \
    ";
    FromJsonString(mOptions.GetOnlineConfig().loadConfigList, jsonStr);

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, "", "", ""));
    string incDocString = "";
    for (size_t i = 0; i < 100 * 1024; ++i)
    {
        incDocString += "cmd=add,pk=" + StringUtil::toString(i) + ",string1=hello;";
    }
    ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));
    ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "", "", ""));
    ASSERT_FALSE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "", "", ""));
}

void ForceReopenInteTest::TestBuildRtAndForceReopenAfterReopenFailed()
{
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));
    string fullDocString = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    string incDocString = "cmd=add,pk=2,string1=hello,long1=2,ts=2";
    ASSERT_TRUE(!psm.Transfer(BUILD_INC_NO_MERGE, incDocString, "", ""));

    string rtDocString =  "cmd=add,pk=3,string1=hello,long1=3,ts=3";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDocString, "index1:hello",
                             "long1=1;long1=3"));

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                             "index1:hello", "long1=1;long1=2;long1=3"));
}

void ForceReopenInteTest::TestForceReopenWithCleanSliceFile()
{
    SCOPED_TRACE("Failed");

    mOptions.GetBuildConfig().maxDocCount = 1;
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string fullDoc = "cmd=add,pk=1,string1=abc,long1=1,updatable_multi_long=1 10,ts=1;"
                     "cmd=add,pk=2,string1=abc,long1=2,updatable_multi_long=2 20,ts=2;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDoc,
                                    "index1:abc", 
                                    "long1=1,updatable_multi_long=1 10;"
                                    "long1=2,updatable_multi_long=2 20"));

    // update doc in on disk index
    // delete doc in on disk index
    string modifyFullDoc = "cmd=update_field,pk=1,long1=10,updatable_multi_long=10 100,ts=10;"
                           "cmd=delete,pk=2,ts=20;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, modifyFullDoc,
                                    "index1:abc",
                                    "long1=10,updatable_multi_long=10 100;"));


    string rtDocs = "cmd=add,pk=3,string1=abc,long1=3,updatable_multi_long=3 30,ts=30;"
                    "cmd=add,pk=4,string1=abc,long1=4,updatable_multi_long=4 40,ts=40;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocs,
                                    "index1:abc", 
                                    "long1=10,updatable_multi_long=10 100;"
                                    "long1=3,updatable_multi_long=3 30;"
                                    "long1=4,updatable_multi_long=4 40"));


    // update doc in rt segment
    // delete doc in rt segment
    string modifyRtDoc = "cmd=update_field,pk=3,long1=30,updatable_multi_long=30 300,ts=300;"
                         "cmd=delete,pk=4,ts=400;";
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, modifyRtDoc,
                                    "index1:abc",
                                    "long1=10,updatable_multi_long=10 100;"
                                    "long1=30,updatable_multi_long=30 300;"));

    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, modifyFullDoc,
                                    "index1:abc",
                                    "long1=10,updatable_multi_long=10 100;"
                                    "long1=30,updatable_multi_long=30 300;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, rtDocs,
                                    "index1:abc",
                                    "long1=10,updatable_multi_long=10 100;"
                                    "long1=30,updatable_multi_long=30 300;"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, modifyRtDoc,
                                    "index1:abc",
                                    "long1=10,updatable_multi_long=10 100;"
                                    "long1=30,updatable_multi_long=30 300;"));
}

void ForceReopenInteTest::TestForceReopenWithUsingReader()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));

    string incDoc = "cmd=add,pk=1,string1=hello,long1=1,ts=1";
    string rtDoc = "cmd=add,pk=2,string1=hello,long1=2,ts=2";

    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=2"));

    IndexPartitionReaderPtr reader = psm.GetIndexPartition()->GetReader();
    ASSERT_TRUE(!psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc, "", ""));
    reader.reset();
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, "",
                             "index1:hello", "long1=1;long1=2"));
}

void ForceReopenInteTest::TestReopenWithRollbackVersion()
{
    DoTestReopenWithRollbackVersion(true);
}

void ForceReopenInteTest::TestReopenIncWithUpdatePatch()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    options.GetBuildConfig().maxDocCount = 1;
    options.GetMergeConfig().mergeStrategyStr = "specific_segments";
    options.GetMergeConfig().mergeStrategyParameter.SetLegacyString(
            "merge_segments=2,3");

    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));
    string incDocs = "cmd=add,pk=1,string1=hello,long1=1,ts=0;" // segment_0
                     "cmd=add,pk=2,string1=hello,long1=1,ts=1;" // segment_1
                     "cmd=update_field,pk=1,long1=3,ts=1;"
                     "cmd=add,pk=3,string1=hello,long1=1,ts=2;" // segment_2
                     "cmd=add,pk=4,string1=hello,long1=1,ts=3;";// segment_3

    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDocs, "", ""));

    OnlinePartitionPtr partition(new OnlinePartition);
    partition->Open(GET_TEST_DATA_PATH(), "", mSchema, options);
    IndexPartitionReaderPtr reader = partition->GetReader();
    AttributeReaderPtr attrReader = reader->GetAttributeReader("long1");
    string value;
    attrReader->Read(0, value, NULL);
    ASSERT_EQ(string("3"), value);

    ASSERT_TRUE(psm.Transfer(BUILD_INC, "", "", "")); // merge segment2,3

    reader.reset();
    attrReader.reset();
    ASSERT_EQ(IndexPartition::OS_OK, partition->ReOpen(true));

    reader = partition->GetReader();
    attrReader = reader->GetAttributeReader("long1");
    attrReader->Read(0, value, NULL);
    ASSERT_EQ(string("3"), value);
}

void ForceReopenInteTest::TestForceReopenMemoryControl()
{
    PartitionStateMachine psm;
    IndexPartitionOptions options;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootPath));

    string incDocString = "cmd=add,pk=1,string1=hello,ts=1";
    ASSERT_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, incDocString,
                             "pk:1", "docid=0"));

    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    OnlinePartitionPtr indexPart = DYNAMIC_POINTER_CAST(OnlinePartition, 
            psm.GetIndexPartition());
    ASSERT_TRUE(indexPart);

    // make index
    DirectoryPtr dirctory = GET_PARTITION_DIRECTORY();
    Version version = VersionMaker::Make(1, "1", "", "");
    version.SetTimestamp(2);
    version.Store(dirctory, false);
    string seg0Path = dirctory->GetPath() + "/segment_0_level_0";
    string seg1Path = dirctory->GetPath() + "/segment_1_level_0";
    
    string cmd = "cp -r -T " + seg0Path + " " + seg1Path;
    system(cmd.c_str());
    // size_t metaSize = calculateMetaSize(directory);
    // force reopen
    auto part = psm.GetIndexPartition();
    auto onlinePart = DYNAMIC_POINTER_CAST(OnlinePartition, part);
    auto fs = onlinePart->GetFileSystem();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN_FORCE, "", "pk:1", "docid=0"));
    fs->CleanCache();
    int64_t memUseAfterForceReopen = 
        indexPart->mResourceCalculator->GetCurrentMemoryUse();
    ASSERT_TRUE(psm.Transfer(PE_REOPEN, "", "", ""));
    fs->CleanCache();
    // expect nothing to clean
    int64_t memUseAfterClean = 
        indexPart->mResourceCalculator->GetCurrentMemoryUse();
    // size_t metaSizeAfterClean = calculateMetaSize(directory);
    IE_LOG(INFO, "%ld %ld", memUseAfterClean, memUseAfterForceReopen);
    ASSERT_EQ(memUseAfterClean / 10240, memUseAfterForceReopen / 10240);
}

void ForceReopenInteTest::TestCleanReadersAndInMemoryIndex()
{
    PartitionStateMachine psm;
    mOptions.GetOnlineConfig().enableAsyncCleanResource = false;
    mOptions.GetBuildConfig().maxDocCount = 1;
    ASSERT_TRUE(psm.Init(mSchema, mOptions, mRootPath));

    string rtDoc = "cmd=add,pk=1,string1=hello,ts=1;";
    ASSERT_TRUE(psm.Transfer(BUILD_RT, rtDoc, "", ""));

    const IndexPartitionPtr& indexPart = psm.GetIndexPartition();
    OnlinePartitionPtr onlinePart = DYNAMIC_POINTER_CAST(
            OnlinePartition, indexPart);
    const IndexlibFileSystemPtr& fs = onlinePart->GetFileSystem();
    DirectoryPtr partitionDir = DirectoryCreator::Get(fs, mRootPath, true);
    DirectoryPtr rtDir = partitionDir->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, true);
    string rtSeg0 = "segment_" + StringUtil::toString(
            RealtimeSegmentDirectory::ConvertToRtSegmentId(0)) + "_level_0";
    // rt segment and version exist before force reopen
    ASSERT_TRUE(rtDir->IsExist(rtSeg0));
    ASSERT_TRUE(rtDir->IsExist("version.0"));

    ASSERT_EQ((size_t)3, onlinePart->mReaderContainer->Size());

    string incDoc = "cmd=add,pk=3,string1=hello,ts=3;";
    // force reopen clean all old readers, and create new reader
    ASSERT_TRUE(psm.Transfer(BUILD_INC_NO_MERGE_FORCE_REOPEN, incDoc, "", ""));
    ASSERT_EQ((size_t)1, onlinePart->mReaderContainer->Size());
    ASSERT_FALSE(rtDir->IsExist(rtSeg0));
    ASSERT_FALSE(rtDir->IsExist("version.0"));
}

IE_NAMESPACE_END(partition);

