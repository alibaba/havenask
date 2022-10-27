#include "indexlib/partition/test/join_segment_writer_unittest.h"
#include "indexlib/partition/test/mock_partition_modifier.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/single_field_partition_data_provider.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, JoinSegmentWriterTest);

JoinSegmentWriterTest::JoinSegmentWriterTest()
{
}

JoinSegmentWriterTest::~JoinSegmentWriterTest()
{
}

void JoinSegmentWriterTest::CaseSetUp()
{
}

void JoinSegmentWriterTest::CaseTearDown()
{
}

// void JoinSegmentWriterTest::TestSimpleProcess()
// {
//     SingleFieldPartitionDataProvider provider;
//     provider.Init(GET_TEST_DATA_PATH(), "int64", SFP_ATTRIBUTE);
//     provider.Build("0,1,2,3,4,5", SFP_OFFLINE);

//     IndexPartitionSchemaPtr schema = provider.GetSchema();
//     IndexPartitionOptions options;
//     options.GetOnlineConfig().maxOperationQueueBlockSize = 3;
//     PartitionDataPtr partitionData = 
//         PartitionDataCreator::CreateBuildingPartitionData(
//                 GET_FILE_SYSTEM(), schema, options);

//     JoinSegmentWriter joinWriter(schema, options, GET_FILE_SYSTEM());
//     OperationQueuePtr operationQueue(new OperationQueue(NULL, false, 3));
//     for (size_t i = 0; i < 10; i++)
//     {
//         MockOperation* mockOperation = MockOperationMaker::MakeOperation(
//                 100 + i, operationQueue->GetPool());
//         operationQueue->AddOperation(mockOperation);

//         EXPECT_CALL(*mockOperation, Process(_)).Times(1);
//         EXPECT_CALL(*mockOperation, GetSegmentId()).WillRepeatedly(Return(0));
//     }

//     joinWriter.mSnapShotOpQueue.reset(operationQueue->SnapShot());
//     MockOperation* mockOperation = MockOperationMaker::MakeOperation(
//             200, operationQueue->GetPool());
//     operationQueue->AddOperation(mockOperation);

//     MockPartitionModifierPtr mockModifier(new MockPartitionModifier(schema));
//     joinWriter.mOperationQueue = operationQueue;
//     joinWriter.mModifier = mockModifier;
//     joinWriter.mOnDiskVersion = partitionData->GetOnDiskVersion();

//     EXPECT_CALL(*mockOperation, Process(_)).Times(0);    
//     EXPECT_CALL(*mockOperation, GetSegmentId()).Times(0);
//     joinWriter.PreJoin();
//     ASSERT_EQ((size_t)10, joinWriter.mRedoOpCount);

//     EXPECT_CALL(*mockOperation, Process(_)).Times(1);    
//     EXPECT_CALL(*mockOperation, GetSegmentId()).WillRepeatedly(Return(0));
//     joinWriter.Join();
//     ASSERT_EQ((size_t)11, joinWriter.mRedoOpCount);

//     segmentid_t joinSegmentId = JoinSegmentDirectory::ConvertToJoinSegmentId(0);
//     EXPECT_CALL(*mockModifier, Dump(_,joinSegmentId)).Times(1);
//     joinWriter.Dump(partitionData);

//     Version version = partitionData->GetVersion();
//     ASSERT_EQ((size_t)2, version.GetSegmentCount());
//     ASSERT_EQ((segmentid_t)joinSegmentId, version[1]);
// }

IE_NAMESPACE_END(partition);

