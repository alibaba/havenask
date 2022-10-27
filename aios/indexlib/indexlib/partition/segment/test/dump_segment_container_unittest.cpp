#include "indexlib/partition/segment/test/dump_segment_container_unittest.h"
#include "indexlib/partition/segment/normal_segment_dump_item.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/document/document.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"


using namespace std;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, DumpSegmentContainerTest);

DumpSegmentContainerTest::DumpSegmentContainerTest()
{
}

DumpSegmentContainerTest::~DumpSegmentContainerTest()
{
}

void DumpSegmentContainerTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    string field = "string1:string;";
    string index = "index1:string:string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    mOptions.SetIsOnline(true);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
    mPartitionData = PartitionDataMaker::CreatePartitionData(
                GET_FILE_SYSTEM(), mOptions, mSchema);
}

void DumpSegmentContainerTest::CaseTearDown()
{
}

void DumpSegmentContainerTest::TestSimpleProcess()
{
    InMemorySegmentPtr inMemSeg = mPartitionData->CreateNewSegment();   //create building segment for dump item
    NormalSegmentDumpItemPtr dumpItem = CreateSegmentDumpItem(mOptions, mSchema, mPartitionData);
    ASSERT_EQ(inMemSeg, dumpItem->GetInMemorySegment());
    
    DumpSegmentContainerPtr container = DumpSegmentContainerPtr(new DumpSegmentContainer);
    ASSERT_EQ(0u, container->GetUnDumpedSegmentSize());
    container->PushDumpItem(dumpItem);
    ASSERT_EQ(1u, container->GetUnDumpedSegmentSize());

    container->ClearDumpedSegment(); // clear dumped segment, no effect for dumping segment
    ASSERT_EQ(1u, container->GetUnDumpedSegmentSize());
    
    ASSERT_EQ(dumpItem, container->GetOneSegmentItemToDump());
    vector<InMemorySegmentPtr> inMemSegments;
    container->GetDumpingSegments(inMemSegments);
    ASSERT_EQ(1u, inMemSegments.size());
    ASSERT_EQ(inMemSeg, inMemSegments.back());
    
    DumpSegmentContainerPtr container2 = DumpSegmentContainerPtr(container->Clone());
    ASSERT_EQ(1u, container2->GetUnDumpedSegmentSize());
    ASSERT_EQ(inMemSeg, container2->GetLastSegment());

    inMemSeg->SetStatus(InMemorySegment::DUMPED);               // dump segment
    ASSERT_EQ(0u, container->GetUnDumpedSegmentSize());
    ASSERT_EQ(1u, container->GetDumpItemSize());
    ASSERT_EQ(inMemSeg, container->GetLastSegment());
    ASSERT_FALSE(container->GetOneSegmentItemToDump());             // no dump segment
    ASSERT_EQ(0u, container2->GetUnDumpedSegmentSize());
    ASSERT_EQ(1u, container2->GetDumpItemSize());
    
    container->ClearDumpedSegment();
    ASSERT_EQ(0u, container->GetUnDumpedSegmentSize());
    ASSERT_EQ(0u, container->GetDumpItemSize());
    ASSERT_FALSE(container->GetLastSegment());
    
    ASSERT_EQ(1u, container2->GetDumpItemSize());
    ASSERT_EQ(inMemSeg, container2->GetLastSegment());
}

void DumpSegmentContainerTest::TestGetMaxDumpingSegmentExpandMemUse()
{
    DumpSegmentContainerPtr container = DumpSegmentContainerPtr(new DumpSegmentContainer);
    mPartitionData->CreateNewSegment();
    OfflinePartitionWriter writer(mOptions, container);
    PartitionModifierPtr modifier =
        PartitionModifierCreator::CreateOfflineModifier(mSchema, mPartitionData,
                true, false);
    writer.Open(mSchema, mPartitionData, modifier);

    string docString = "cmd=add,string1=hello";
    document::NormalDocumentPtr document = DocumentCreator::CreateDocument(mSchema, docString);

    writer.AddDocument(document);
    uint64_t exSize1 = writer.GetBuildingSegmentDumpExpandSize();
    ASSERT_TRUE(exSize1 > 0);
    writer.DumpSegment();
    ASSERT_EQ(1u, container->GetUnDumpedSegmentSize());
    
    mPartitionData->CreateNewSegment();
    PartitionModifierPtr modifier2 =
        PartitionModifierCreator::CreateOfflineModifier(
                    mSchema, mPartitionData, true, false);
    writer.ReOpenNewSegment(modifier2);
    
    string docString2 = "cmd=add,string1=thanksyouverymuch";
    document = DocumentCreator::CreateDocument(mSchema, docString2);    
    writer.AddDocument(document);
    
    uint64_t exSize2 = writer.GetBuildingSegmentDumpExpandSize();
    writer.DumpSegment();
    cout << "=========================" << exSize1 << ", " << exSize2 << endl;
    ASSERT_EQ(max(exSize1, exSize2), container->GetMaxDumpingSegmentExpandMemUse());
    writer.Close();
}

NormalSegmentDumpItemPtr DumpSegmentContainerTest::CreateSegmentDumpItem(
        IndexPartitionOptions& options, IndexPartitionSchemaPtr& schema,
        PartitionDataPtr& partitionData)
{
    
    NormalSegmentDumpItemPtr segmentDumpItem = NormalSegmentDumpItemPtr(new NormalSegmentDumpItem(
                        options, schema, ""));
    segmentDumpItem->Init(NULL, partitionData, PartitionModifierPtr(),
                          FlushedLocatorContainerPtr(), false);
    return segmentDumpItem;
}

IE_NAMESPACE_END(partition);

