#include "indexlib/partition/test/multi_thread_building_partition_data_unittest.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/util/memory_control/memory_quota_controller_creator.h"

using namespace std;
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, MultiThreadBuildingPartitionDataTest);

MultiThreadBuildingPartitionDataTest::MultiThreadBuildingPartitionDataTest()
{
}

MultiThreadBuildingPartitionDataTest::~MultiThreadBuildingPartitionDataTest()
{
}

void MultiThreadBuildingPartitionDataTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";

    string attribute = "string1;price";
    IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, attribute, "");

    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.SetEnablePackageFile(false);

    PartitionStateMachine psm;
    ASSERT_TRUE(psm.Init(schema, options, GET_TEST_DATA_PATH()));
    ASSERT_TRUE(psm.Transfer(BUILD_FULL, "", "", ""));

    DumpSegmentContainerPtr dumpContainer(new DumpSegmentContainer);
    mPartitionData = PartitionDataCreator::CreateBuildingPartitionData(
            GET_FILE_SYSTEM(), schema, options,
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController(), dumpContainer);
}

void MultiThreadBuildingPartitionDataTest::CaseTearDown()
{
}

void MultiThreadBuildingPartitionDataTest::TestSimpleProcess()
{
    DoMultiThreadTest(5, 20);
}

void MultiThreadBuildingPartitionDataTest::DoWrite()
{
    while (!IsFinished())
    {
        InMemorySegmentPtr inMemSegment = mPartitionData->CreateNewSegment();
        DirectoryPtr dir = inMemSegment->GetDirectory();
        inMemSegment->SetStatus(InMemorySegment::DUMPING);
        SegmentInfo segInfo;
        segInfo.Store(dir);
        usleep(10);
    }
}

void MultiThreadBuildingPartitionDataTest::DoRead(int* status)
{
    while (!IsFinished())
    {
        PartitionDataPtr clonePartData(mPartitionData->Clone());
        PartitionData::Iterator iter = clonePartData->Begin();
        segmentid_t segId = INVALID_SEGMENTID;
        for (; iter != clonePartData->End(); ++iter)
        {
            const SegmentData& segData = *iter;
            segmentid_t curSegId = segData.GetSegmentId();
            EXPECT_EQ(curSegId, segId + 1) << curSegId;
            segId = curSegId;
        }
    }
}

IE_NAMESPACE_END(partition);

