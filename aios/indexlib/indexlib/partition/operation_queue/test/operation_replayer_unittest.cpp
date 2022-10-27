#include "indexlib/partition/operation_queue/test/operation_replayer_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationReplayerTest);

OperationReplayerTest::OperationReplayerTest()
{
}

OperationReplayerTest::~OperationReplayerTest()
{
}

void OperationReplayerTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
        "pk:primarykey64:string1";

    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
    mRootDir = GET_TEST_DATA_PATH();
    mOptions.SetIsOnline(true);
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    mOptions.GetOnlineConfig().SetEnableRedoSpeedup(true);
    mOptions.GetOnlineConfig().SetVersionTsAlignment(1); 
}

void OperationReplayerTest::CaseTearDown()
{
}

void OperationReplayerTest::TestSimpleProcess()
{
    mOptions.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    string fullDocString =
        "cmd=add,string1=hello,locator=-1,price=0,ts=0;";

    string rtDocString =
        "cmd=update_field,string1=hello,price=5,ts=6;"
        "cmd=update_field,string1=hello,price=7,ts=8;";

    // make psm destruction, so it will dump rt segments
    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, mOptions, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    
    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(mOptions, mSchema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            mSchema, partReader);

    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);
    OperationReplayer opReplayer(partitionData, mSchema,
                                 util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    
    ASSERT_TRUE(opReplayer.RedoOperations(modifier, onDiskVersion, OperationRedoStrategyPtr()));
    OperationCursor cursor = opReplayer.GetCuror();
    ASSERT_EQ(RealtimeSegmentDirectory::ConvertToRtSegmentId(0), cursor.segId);
    ASSERT_EQ(1, cursor.pos);

    AttributeReaderPtr attrReader = partReader->GetAttributeReader("price");
    string value;
    ASSERT_TRUE(attrReader->Read(0, value));
    ASSERT_EQ(string("7"), value);
}

void OperationReplayerTest::TestRedoFromCursor()
{
    InnerTestRedoFromCursor("2,3,4,5", 6, 0);
    
    // test partitionData has no in-mem segment
    InnerTestRedoFromCursor("2,3,4,5", -1, 0);
    
    // test in-mem segment has no operations
    InnerTestRedoFromCursor("2,3,4,5", 0, 0);

    // test has no normal segments
    InnerTestRedoFromCursor("", 5, 0);
}

void OperationReplayerTest::TestRedoFilterByTimestamp()
{
    for (int64_t i = 0; i <= 20; i++)
    {
        InnerTestRedoFromCursor("2,3,4,5", 6, i);
    }
}

// normalOpStr: opCount1,opCount2,opCount3
void OperationReplayerTest::InnerTestRedoFromCursor(
    const string& normalOpStr, int inMemOpCount, int64_t reclaimOpTimestamp)
{
    TearDown();
    SetUp();
    
    OperationTestMetas opTestMeta;
    PartitionStateMachine psm;
    PartitionDataPtr partData = MakeData(psm, normalOpStr, inMemOpCount, opTestMeta);

    // test redo from invalid begin cursor
    OperationCursor expectLastCursor = opTestMeta.rbegin()->opCursor;
    size_t expectRedoCount = GetRedoOpCount(opTestMeta, 0,
                                            reclaimOpTimestamp);
    CheckRedo(reclaimOpTimestamp, partData,
              OperationCursor(INVALID_SEGMENTID, -1),
              expectLastCursor, expectRedoCount);

    // test redo from cursor of each operation
    for (size_t i = 0; i < opTestMeta.size(); ++i)
    {
        expectRedoCount = GetRedoOpCount(opTestMeta, i + 1,
                                         reclaimOpTimestamp);
        CheckRedo(reclaimOpTimestamp, partData,
                  opTestMeta[i].opCursor, expectLastCursor, expectRedoCount);
    }

    // test redo from invalid end cursor
    OperationCursor invalidEndCursor = expectLastCursor;
    invalidEndCursor.pos++;
    CheckRedo(reclaimOpTimestamp, partData,
              invalidEndCursor, invalidEndCursor, 0);

    invalidEndCursor = expectLastCursor;
    invalidEndCursor.segId++;
    invalidEndCursor.pos = 0;
    CheckRedo(reclaimOpTimestamp, partData,
              invalidEndCursor, invalidEndCursor, 0);
}

void OperationReplayerTest::CheckRedo(
    int64_t reclaimOpTimestamp, const PartitionDataPtr& partData,
    const OperationCursor& opCursor, const OperationCursor& expectLastCursor,
    size_t expectRedoCount)
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    PartitionDataPtr incPartData = PartitionDataCreator::CreateOnDiskPartitionData(
        GET_FILE_SYSTEM(), mSchema, INVALID_VERSION, "", true);

    Version onDiskVersion = incPartData->GetVersion();
    onDiskVersion.SetTimestamp(reclaimOpTimestamp);
        
    PartitionModifierPtr modifier = PartitionModifierCreator::CreatePatchModifier(
        mSchema, incPartData, true, false);
        
    OperationReplayer replayer(partData, mSchema,
                               util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    replayer.Seek(opCursor);
    ASSERT_TRUE(replayer.RedoOperations(modifier, onDiskVersion, OperationRedoStrategyPtr()));
    OperationCursor lastCursor = replayer.GetCuror();
    ASSERT_EQ(expectLastCursor, lastCursor)
        << "expect:" << expectLastCursor.segId << "|" << expectLastCursor.pos
        << ",actural:" << lastCursor.segId << "|" << lastCursor.pos;
        
    ASSERT_EQ(expectRedoCount, replayer.GetRedoCount());
}

size_t OperationReplayerTest::GetRedoOpCount(
    const OperationTestMetas& opTestMetas,
    size_t beginPos, int64_t reclaimOpTimestamp)
{
    size_t redoCount = 0;
    for (size_t i = beginPos; i < opTestMetas.size(); ++i)
    {
        if (opTestMetas[i].opTs >= reclaimOpTimestamp)
        {
            ++redoCount;
        }
    }
    return redoCount;
}

PartitionDataPtr OperationReplayerTest::MakeData(
    PartitionStateMachine& psm,
    const string& normalOpStr, int inMemOpCount,
    OperationTestMetas& opTestMetas)
{
    assert(opTestMetas.empty());
    vector<uint32_t> opCounts;
    StringUtil::fromString(normalOpStr, opCounts, ",");

    IndexPartitionOptions options;
    // make in memory operation writer multi operation blocks
    options.GetOnlineConfig().maxOperationQueueBlockSize = 2;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = true;
    
    psm.Init(mSchema, options, GET_TEST_DATA_PATH());
    int64_t timestamp = 1;
    // build normal rt segments
    for (size_t i = 0; i < opCounts.size(); ++i)
    {
        stringstream ss;
        for (size_t j = 0; j < opCounts[i]; ++j)
        {
            ss << "cmd=add,string1=hello,price=0,ts=" << timestamp << ";";
            OperationTestMeta opMeta;
            opMeta.opCursor = OperationCursor(
                RealtimeSegmentDirectory::ConvertToRtSegmentId(i), j);
            opMeta.opTs = timestamp++;
            opTestMetas.push_back(opMeta);
        }
        psm.Transfer(BUILD_RT_SEGMENT, ss.str(), "", "");
    }

    // build in-memory rt segment
    stringstream ss;
    for (int i = 0; i < inMemOpCount; ++i)
    {
        ss << "cmd=add,string1=hello,price=0,ts=" << timestamp << ";";
        OperationTestMeta opMeta;
        opMeta.opCursor = OperationCursor(
            RealtimeSegmentDirectory::ConvertToRtSegmentId(
                (segmentid_t)opCounts.size()), i);
        opMeta.opTs = timestamp++;
        opTestMetas.push_back(opMeta);
    }
    psm.Transfer(BUILD_RT, ss.str(), "", "");

    IndexPartitionPtr partition = psm.GetIndexPartition();
    assert(partition);
    OnlinePartitionPtr onlinePartition = DYNAMIC_POINTER_CAST(
        OnlinePartition, partition);
    PartitionDataPtr partitionData = onlinePartition->GetPartitionData();
    if (inMemOpCount < 0)
    {
        InMemorySegmentPtr inMemSegment = partitionData->GetInMemorySegment();
        assert(inMemSegment);
        inMemSegment->SetOperationWriter(OperationWriterPtr());
    }
    return partitionData;
}

void OperationReplayerTest::TestRedoStrategy()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1); 
    string fullDocString =
        "cmd=add,string1=pk0,price=0,ts=0;"
        "cmd=add,string1=pk1,price=1,ts=0;"
        "cmd=add,string1=pk2,price=2,ts=0;"
        "cmd=add,string1=pk3,price=3,ts=0;"
        "cmd=add,string1=pk4,price=4,ts=0;";    

    string rtDocString =
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=150;"        
        "cmd=update_field,string1=pk0,price=10,ts=100;"
        "cmd=update_field,string1=pk1,price=11,ts=120;"
        "cmd=update_field,string1=pk2,price=12,ts=150;"
        "cmd=update_field,string1=pk3,price=13,ts=180;"
        "cmd=update_field,string1=pk4,price=14,ts=190;";    

    string incDocString =
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=150;"
        "##stopTs=100;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersion(mRootDir, fullVersion, INVALID_VERSION);
    ASSERT_TRUE(fullVersion.GetVersionId() != INVALID_VERSION);
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    
    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            mSchema, partReader);

    
    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);

    OnDiskPartitionDataPtr incPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
            onlinePart->GetFileSystem(), mSchema, onDiskVersion, "", true);
    
    OperationRedoStrategyPtr redoStrategy(new OperationRedoStrategy());
    redoStrategy->Init(incPartitionData, fullVersion, options.GetOnlineConfig(), mSchema);

    OperationReplayer opReplayer(partitionData, mSchema,
                                 util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    ASSERT_TRUE(opReplayer.RedoOperations(modifier, onDiskVersion, redoStrategy));
    ASSERT_EQ(4u, opReplayer.GetRedoCount());
    const OperationRedoCounter& redoCounter = redoStrategy->GetCounter();
    ASSERT_EQ(5u, redoCounter.updateOpCount);
    ASSERT_EQ(1u, redoCounter.deleteOpCount);
    ASSERT_EQ(0u, redoCounter.otherOpCount);
    ASSERT_EQ(2u, redoCounter.skipRedoUpdateOpCount);
    ASSERT_EQ(0u, redoCounter.skipRedoDeleteOpCount);
    ASSERT_EQ(0u, redoCounter.hintOpCount);
}

void OperationReplayerTest::TestRedoStrategyDisableRedoSpeedup()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    options.GetOnlineConfig().SetEnableRedoSpeedup(false);
    options.GetOnlineConfig().SetVersionTsAlignment(1);    
    string fullDocString =
        "cmd=add,string1=pk0,price=0,ts=0;"
        "cmd=add,string1=pk1,price=1,ts=0;"
        "cmd=add,string1=pk2,price=2,ts=0;"
        "cmd=add,string1=pk3,price=3,ts=0;"
        "cmd=add,string1=pk4,price=4,ts=0;";    

    string rtDocString =
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=150;"        
        "cmd=update_field,string1=pk0,price=10,ts=100;"
        "cmd=update_field,string1=pk1,price=11,ts=120;"
        "cmd=update_field,string1=pk2,price=12,ts=150;"
        "cmd=update_field,string1=pk3,price=13,ts=180;"
        "cmd=update_field,string1=pk4,price=14,ts=190;";    

    string incDocString =
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=150;"        
        "##stopTs=100;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersion(mRootDir, fullVersion, INVALID_VERSION);
    ASSERT_TRUE(fullVersion.GetVersionId() != INVALID_VERSION);
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    
    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            mSchema, partReader);

    
    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);

    OnDiskPartitionDataPtr incPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
            onlinePart->GetFileSystem(), mSchema, onDiskVersion, "", true);
    
    OperationRedoStrategyPtr redoStrategy(new OperationRedoStrategy());
    redoStrategy->Init(incPartitionData, fullVersion, options.GetOnlineConfig(), mSchema);

    OperationReplayer opReplayer(partitionData, mSchema,
                                 util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    ASSERT_TRUE(opReplayer.RedoOperations(modifier, onDiskVersion, redoStrategy));
    ASSERT_EQ(6u, opReplayer.GetRedoCount()); 
}

void OperationReplayerTest::TestRedoStrategyWithRemoveOperation()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1);        
    string fullDocString =
        "cmd=add,string1=pk0,price=0,ts=0;"
        "cmd=add,string1=pk1,price=1,ts=0;"
        "cmd=add,string1=pk2,price=2,ts=0;"
        "cmd=add,string1=pk3,price=3,ts=0;"
        "cmd=add,string1=pk4,price=4,ts=50;";    

    // rtDoc will generate 5 delOp {pk4,pk0,pk1,pk2,pk3}
    string rtDocString =
        "cmd=add,string1=pk4,price=4,ts=50;"
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=130;" 
        "cmd=delete,string1=pk0,ts=100;"
        "cmd=delete,string1=pk1,ts=120;"
        "cmd=delete,string1=pk2,ts=150;"
        "cmd=delete,string1=pk3,ts=180;";

    //version.ts = 100, version.maxTs = 130
    //delOp{pk9, pk0,pk1} will be redo
    string incDocString =
        "cmd=add,string1=pk4,price=4,ts=50;"        
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=130;" 
        "cmd=delete,string1=pk0,ts=100;" 
        "cmd=delete,string1=pk1,ts=120;" 
        "##stopTs=100;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersion(mRootDir, fullVersion, INVALID_VERSION);
    ASSERT_TRUE(fullVersion.GetVersionId() != INVALID_VERSION);
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    
    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            mSchema, partReader);
    
    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);

    OnDiskPartitionDataPtr incPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
            onlinePart->GetFileSystem(), mSchema, onDiskVersion, "", true);    

    OperationRedoStrategyPtr redoStrategy(new OperationRedoStrategy());
    redoStrategy->Init(incPartitionData, fullVersion, options.GetOnlineConfig(), mSchema);
    
    OperationReplayer opReplayer(partitionData, mSchema,
                                 util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    ASSERT_TRUE(opReplayer.RedoOperations(modifier, onDiskVersion, redoStrategy));
    ASSERT_EQ(3u, opReplayer.GetRedoCount());     
}

void OperationReplayerTest::TestRedoRmOpWithIncInconsistentWithRt()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1); 
    options.GetOnlineConfig().isIncConsistentWithRealtime = false;
    options.GetBuildConfig(false).maxDocCount = 2;    
    string fullDocString =
        "cmd=add,string1=pk0,price=0,ts=0;"
        "cmd=add,string1=pk1,price=1,ts=0;"
        "cmd=add,string1=pk2,price=2,ts=0;"
        "cmd=add,string1=pk3,price=3,ts=0;"
        "cmd=add,string1=pk4,price=4,ts=50;";
    
    string rtDocString =
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=delete,string1=pk0,ts=100;"
        "cmd=add,string1=pk9,price=90,ts=130;" 
        "cmd=delete,string1=pk1,ts=120;"
        "cmd=delete,string1=pk2,ts=150;"
        "cmd=delete,string1=pk3,ts=180;"
        "cmd=delete,string1=pk4,ts=111;"
        "cmd=delete,string1=pk5,ts=112;"
        "cmd=add,string1=pk6,price=66,ts=113;" 
        "cmd=delete,string1=pk9,ts=114;"; 

    string incDocString =
        "cmd=add,string1=pk4,price=4,ts=50;"        
        "cmd=add,string1=pk5,price=50,ts=50;"
        "cmd=add,string1=pk2,price=20,ts=51;"
        "cmd=add,string1=pk3,price=30,ts=52;" 
        "cmd=add,string1=pk7,price=70,ts=70;"
        "cmd=add,string1=pk6,price=60,ts=60;"
        "cmd=add,string1=pk8,price=80,ts=80;"
        "cmd=add,string1=pk9,price=90,ts=130;" 
        "cmd=delete,string1=pk0,ts=100;" 
        "cmd=delete,string1=pk1,ts=120;" 
        "##stopTs=100;";

    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(mSchema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));

    Version fullVersion;
    VersionLoader::GetVersion(mRootDir, fullVersion, INVALID_VERSION);
    ASSERT_TRUE(fullVersion.GetVersionId() != INVALID_VERSION);
    
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT_SEGMENT, rtDocString, "", ""));
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDocString, "", ""));
    
    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(options, mSchema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            mSchema, partReader);
    
    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);

    OnDiskPartitionDataPtr incPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
            onlinePart->GetFileSystem(), mSchema, onDiskVersion, "", true);    

    OperationRedoStrategyPtr redoStrategy(new OperationRedoStrategy());
    redoStrategy->Init(incPartitionData, fullVersion, options.GetOnlineConfig(), mSchema);

    OperationIterator iter(partitionData, mSchema);
    OperationCursor cursor;    
    iter.Init(100, cursor);
    OperationRedoHint redoHint;
    OperationBase* operation;    
    ASSERT_TRUE(iter.HasNext());
    // pk0
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_FALSE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    // pk9
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    // pk1
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_FALSE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    // pk2
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_TRUE(redoHint.IsValid());
    EXPECT_EQ(4, redoHint.GetSegmentId());
    EXPECT_EQ(0, redoHint.GetLocalDocId());
    // pk3
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_TRUE(redoHint.IsValid());
    EXPECT_EQ(4, redoHint.GetSegmentId());
    EXPECT_EQ(1, redoHint.GetLocalDocId());
    // pk4
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_TRUE(redoHint.IsValid());
    EXPECT_EQ(3, redoHint.GetSegmentId());
    EXPECT_EQ(0, redoHint.GetLocalDocId());
    // pk5
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    // pk6
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    // pk9
    redoHint.Reset();
    operation = iter.Next();
    EXPECT_TRUE(redoStrategy->NeedRedo(operation, redoHint));
    EXPECT_FALSE(redoHint.IsValid());
    ASSERT_FALSE(iter.HasNext());
}

void OperationReplayerTest::TestOpTargetSegIdUpdated()
{
    IndexPartitionOptions options = mOptions;
    options.GetOnlineConfig().onDiskFlushRealtimeIndex = false;
    options.GetOnlineConfig().isIncConsistentWithRealtime = true;
    options.GetOnlineConfig().SetEnableRedoSpeedup(true);
    options.GetOnlineConfig().SetVersionTsAlignment(1); 
    options.GetBuildConfig(false).maxDocCount = 2;

    string fullDocString = "";

    string field = "pk:uint64:pk;string1:string;text1:text;long1:uint32;";
    string index = "pk:primarykey64:pk;index1:string:string1;";
    string attr = "long1;";
    auto schema = SchemaMaker::MakeSchema(field, index, 
            attr, ""); 

    string rtDoc =
        "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
        "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
        "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
        "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
        "cmd=add,pk=5,string1=hello,long1=5,ts=120;" 
        "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
        "cmd=add,pk=7,string1=hello,long1=7,ts=120;"
        "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
        
        "cmd=add,pk=1,string1=hello,long1=1,ts=200;"
        "cmd=add,pk=2,string1=hello,long1=2,ts=200;"
        "cmd=add,pk=3,string1=hello,long1=3,ts=200;"
        "cmd=add,pk=4,string1=hello,long1=4,ts=310;"
        "cmd=add,pk=5,string1=hello,long1=5,ts=320;" 
        "cmd=add,pk=6,string1=hello,long1=6,ts=320;"
        "cmd=add,pk=7,string1=hello,long1=7,ts=320;"
        "cmd=add,pk=8,string1=hello,long1=8,ts=320;";

    string incDoc1 =
        "cmd=add,pk=1,string1=hello,long1=1,ts=0;"
        "cmd=add,pk=2,string1=hello,long1=2,ts=0;"
        "cmd=add,pk=3,string1=hello,long1=3,ts=0;"
        "cmd=add,pk=4,string1=hello,long1=4,ts=110;"
        "cmd=add,pk=5,string1=hello,long1=5,ts=120;" 
        "cmd=add,pk=6,string1=hello,long1=6,ts=120;"
        "cmd=add,pk=7,string1=hello,long1=7,ts=120;"
        "cmd=add,pk=8,string1=hello,long1=8,ts=120;"
        "##stopTs=120;";

    string incDoc2 =
        "cmd=add,pk=1,string1=hello,long1=1,ts=200;"
        "cmd=add,pk=2,string1=hello,long1=2,ts=200;"
        "cmd=add,pk=3,string1=hello,long1=3,ts=200;"
        "cmd=add,pk=4,string1=hello,long1=4,ts=310;"
        "##stopTs=311;";


    PartitionStateMachine psm;
    INDEXLIB_TEST_TRUE(psm.Init(schema, options, mRootDir));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_FULL_NO_MERGE, fullDocString, "", ""));        
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_RT, rtDoc, "index1:hello", "long1=1;2;3;4;5;6;7;8"));
    INDEXLIB_TEST_TRUE(psm.Transfer(BUILD_INC_NO_MERGE, incDoc1, "index1:hello", "long1=1;2;3;4;5;6;7;8"));    
    INDEXLIB_TEST_TRUE(psm.Transfer(PE_BUILD_INC, incDoc2, "", ""));

    // load partition data
    DumpSegmentContainerPtr container(new DumpSegmentContainer);
    OnlinePartitionPtr onlinePart =
        DYNAMIC_POINTER_CAST(OnlinePartition, psm.GetIndexPartition());
    BuildingPartitionDataPtr partitionData =
        DYNAMIC_POINTER_CAST(BuildingPartitionData, onlinePart->GetPartitionData());
    
    OnlinePartitionReaderPtr partReader(new OnlinePartitionReader(options, schema, util::SearchCachePartitionWrapperPtr()));
    partReader->Open(partitionData);

    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(
            schema, partReader);
    
    Version onDiskVersion;
    VersionLoader::GetVersion(mRootDir, onDiskVersion, INVALID_VERSION);

    OnDiskPartitionDataPtr incPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
            onlinePart->GetFileSystem(), schema, onDiskVersion, "", true);    

    Version fullVersion;
    VersionLoader::GetVersion(mRootDir, fullVersion, INVALID_VERSION);
    ASSERT_TRUE(fullVersion.GetVersionId() != INVALID_VERSION);
    
    OperationRedoStrategyPtr redoStrategy(new OperationRedoStrategy());
    redoStrategy->Init(incPartitionData, fullVersion, options.GetOnlineConfig(), schema);
    
    OperationReplayer opReplayer(partitionData, schema,
                                 util::MemoryQuotaControllerCreator::CreateSimpleMemoryController());
    ASSERT_TRUE(opReplayer.RedoOperations(modifier, onDiskVersion, redoStrategy));
    ASSERT_EQ(0u, opReplayer.GetRedoCount());
}

IE_NAMESPACE_END(partition);

