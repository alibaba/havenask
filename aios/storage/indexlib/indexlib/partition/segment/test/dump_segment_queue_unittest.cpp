#include "indexlib/partition/segment/test/dump_segment_queue_unittest.h"

#include "indexlib/document/document.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::common;
using namespace indexlib::index;
using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::table;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, DumpSegmentQueueTest);

class MockCustomSegmentDumpItem : public CustomSegmentDumpItem
{
public:
    MockCustomSegmentDumpItem(const config::IndexPartitionOptions& options,
                              const config::IndexPartitionSchemaPtr& schema, const std::string& partitionName)
        : CustomSegmentDumpItem(options, schema, partitionName)
    {
    }
    MOCK_METHOD(void, Dump, (), (override));
    MOCK_METHOD(bool, DumpWithMemLimit, (), (override));
    MOCK_METHOD(bool, IsDumped, (), (const, override));
    MOCK_METHOD(uint64_t, GetEstimateDumpSize, (), (const, override));
    MOCK_METHOD(size_t, GetInMemoryMemUse, (), (const, override));
    MOCK_METHOD(segmentid_t, GetSegmentId, (), (const, override));
    MOCK_METHOD(table::TableWriterPtr, GetTableWriter, (), (const));
    MOCK_METHOD(index_base::SegmentInfoPtr, GetSegmentInfo, (), (const, override));
};

DumpSegmentQueueTest::DumpSegmentQueueTest() {}

DumpSegmentQueueTest::~DumpSegmentQueueTest() {}

void DumpSegmentQueueTest::CaseSetUp()
{
    mRootDir = GET_TEMP_DATA_PATH();
    string field = "string1:string;";
    string index = "index1:string:string1;";
    mSchema = SchemaMaker::MakeSchema(field, index, "", "");
    mOptions.SetIsOnline(true);
    mOptions.GetOnlineConfig().enableAsyncDumpSegment = true;
}

void DumpSegmentQueueTest::CaseTearDown() {}

void DumpSegmentQueueTest::TestBase()
{
    // Test for Size() / PushDumpItem() / GetDumpedItems() / Clear()
    std::vector<CustomSegmentDumpItemPtr> itemList;
    size_t itemCount = 3;
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, "test");
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, "test");
    MockCustomSegmentDumpItem* mockItem3 = new MockCustomSegmentDumpItem(mOptions, mSchema, "test");
    EXPECT_CALL(*mockItem1, IsDumped()).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockItem2, IsDumped()).Times(1).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockItem3, IsDumped()).Times(1).WillRepeatedly(Return(true));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    ASSERT_EQ(0u, queue->Size());

    queue->GetDumpedItems(itemList);
    ASSERT_EQ(0u, itemList.size());

    auto seg_invalid = queue->GetLastSegment();
    ASSERT_TRUE(seg_invalid == nullptr);

    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem3));

    ASSERT_EQ(itemCount, queue->Size());

    queue->GetDumpedItems(itemList);
    ASSERT_EQ(itemCount, itemList.size());

    auto seg_valid = queue->GetLastSegment();
    ASSERT_TRUE(seg_valid != nullptr);

    // after clear
    itemList.clear();
    queue->Clear();
    ASSERT_EQ(0u, queue->Size());
    queue->GetDumpedItems(itemList);
    ASSERT_EQ(0u, itemList.size());

    seg_invalid = queue->GetLastSegment();
    ASSERT_TRUE(seg_invalid == nullptr);
}

void DumpSegmentQueueTest::TestGetLastSegment()
{
    std::vector<CustomSegmentDumpItemPtr> itemList;
    size_t itemCount = 3;
    std::string testPartName("test3");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, "test1");
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, "test2");
    MockCustomSegmentDumpItem* mockItem3 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName);

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    auto segItem = queue->GetLastSegment();
    ASSERT_TRUE(!segItem);

    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem3));

    ASSERT_EQ(itemCount, queue->Size());

    segItem = queue->GetLastSegment();
    ASSERT_TRUE(segItem != nullptr);
    ASSERT_TRUE(segItem->mPartitionName == testPartName);
}

void DumpSegmentQueueTest::TestClone()
{
    std::vector<CustomSegmentDumpItemPtr> itemListOld;
    std::vector<CustomSegmentDumpItemPtr> itemListNew;
    size_t itemCount = 2;
    std::string testPartName1("test1");
    std::string testPartName2("test2");
    std::string testPartName3("test3");

    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);
    MockCustomSegmentDumpItem* mockItem3 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName3);
    EXPECT_CALL(*mockItem1, IsDumped()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockItem2, IsDumped()).WillRepeatedly(Return(true));
    EXPECT_CALL(*mockItem3, IsDumped()).WillRepeatedly(Return(false));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem3));

    DumpSegmentQueuePtr newQueue(queue->Clone());

    newQueue->GetDumpedItems(itemListNew);
    queue->GetDumpedItems(itemListOld);

    ASSERT_EQ(queue->Size(), newQueue->Size());
    ASSERT_EQ(itemListNew.size(), itemListOld.size());
    ASSERT_EQ(itemCount, itemListOld.size());
    size_t idx = 0;
    auto oldBack = itemListOld[idx];
    auto newBack = itemListNew[idx];
    ASSERT_TRUE(oldBack->mPartitionName == newBack->mPartitionName);
    ASSERT_TRUE(oldBack->mPartitionName == testPartName1);

    ++idx;
    auto oldFront = itemListOld[idx];
    auto newFront = itemListNew[idx];
    ASSERT_TRUE(oldFront->mPartitionName == newFront->mPartitionName);
    ASSERT_TRUE(oldFront->mPartitionName == testPartName2);
}

void DumpSegmentQueueTest::TestPopDumpedItems()
{
    std::vector<CustomSegmentDumpItemPtr> itemList;
    size_t itemCount = 2;
    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);
    EXPECT_CALL(*mockItem1, IsDumped()).Times(2).WillOnce(Return(false)).WillOnce(Return(true));

    EXPECT_CALL(*mockItem2, IsDumped()).Times(3).WillOnce(Return(false)).WillOnce(Return(false)).WillOnce(Return(true));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    ASSERT_EQ(itemCount, queue->Size());
    queue->PopDumpedItems();
    ASSERT_EQ(itemCount, queue->Size());

    queue->PopDumpedItems();
    ASSERT_EQ(itemCount - 1, queue->Size());

    queue->PopDumpedItems();
    ASSERT_EQ(itemCount - 1, queue->Size());

    queue->PopDumpedItems();
    ASSERT_EQ(0, queue->Size());
}

void DumpSegmentQueueTest::TestProcessOneItem()
{
    std::vector<CustomSegmentDumpItemPtr> itemList;
    size_t itemCount = 2;
    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);
    EXPECT_CALL(*mockItem1, IsDumped()).Times(2).WillOnce(Return(true)).WillOnce(Return(false));

    EXPECT_CALL(*mockItem1, Dump()).Times(1).WillRepeatedly(Return());

    // empty queue
    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    bool hasNewDumpedSegment = false;
    ASSERT_EQ(0, queue->Size());
    auto ret = queue->ProcessOneItem(hasNewDumpedSegment);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(hasNewDumpedSegment);

    // not empty, buf dumped
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    ASSERT_EQ(itemCount, queue->Size());
    ret = queue->ProcessOneItem(hasNewDumpedSegment);
    ASSERT_TRUE(ret);
    ASSERT_FALSE(hasNewDumpedSegment);

    // not empty, buf not dumped
    hasNewDumpedSegment = false;
    ret = queue->ProcessOneItem(hasNewDumpedSegment);
    ASSERT_TRUE(ret);
    ASSERT_TRUE(hasNewDumpedSegment);
}

void DumpSegmentQueueTest::TestGetEstimateDumpSize()
{
    std::vector<CustomSegmentDumpItemPtr> itemList;
    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);

    size_t itemSize = 100;
    EXPECT_CALL(*mockItem1, GetEstimateDumpSize()).WillRepeatedly(Return(itemSize));
    EXPECT_CALL(*mockItem2, GetEstimateDumpSize()).WillRepeatedly(Return(itemSize));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    auto result = queue->GetEstimateDumpSize();
    ASSERT_EQ(itemSize * queue->Size(), result);
}

void DumpSegmentQueueTest::TestGetDumpingSegmentsSize()
{
    std::vector<CustomSegmentDumpItemPtr> itemList;
    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);

    size_t perItemDumpSize = 100;
    size_t perItemMemUse = 10;
    EXPECT_CALL(*mockItem1, GetEstimateDumpSize()).Times(1).WillOnce(Return(perItemDumpSize));

    EXPECT_CALL(*mockItem1, GetInMemoryMemUse()).Times(1).WillOnce(Return(perItemMemUse));
    EXPECT_CALL(*mockItem2, GetEstimateDumpSize()).Times(1).WillOnce(Return(perItemDumpSize));

    EXPECT_CALL(*mockItem2, GetInMemoryMemUse()).Times(1).WillOnce(Return(perItemMemUse));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    auto result = queue->GetDumpingSegmentsSize();

    ASSERT_EQ((perItemMemUse + perItemDumpSize) * queue->Size(), result);
}

void DumpSegmentQueueTest::TestReclaimSegments()
{
    index_base::SegmentInfoPtr segInfo1(new index_base::SegmentInfo());
    segInfo1->timestamp = 10;
    index_base::SegmentInfoPtr segInfo2(new index_base::SegmentInfo());
    segInfo2->timestamp = 100;

    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);

    EXPECT_CALL(*mockItem1, GetSegmentInfo()).Times(1).WillRepeatedly(Return(segInfo1));
    EXPECT_CALL(*mockItem2, GetSegmentInfo()).Times(2).WillRepeatedly(Return(segInfo2));

    EXPECT_CALL(*mockItem1, GetSegmentId()).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(*mockItem2, GetSegmentId()).Times(1).WillRepeatedly(Return(1));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    ASSERT_EQ(queue->Size(), 0);

    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    queue->ReclaimSegments(-1);
    ASSERT_EQ(2, queue->Size());

    // will erase first item
    queue->ReclaimSegments(50);
    ASSERT_EQ(1, queue->Size());

    // will erase last item
    queue->ReclaimSegments(200);
    ASSERT_EQ(0, queue->Size());
}

namespace {
class UTTableWriter : public TableWriter
{
    bool DoInit() override { return true; }
    BuildResult Build(docid_t docId, const document::DocumentPtr& doc) override { return BuildResult::BR_OK; }
    bool IsDirty() const override { return true; }
    bool DumpSegment(BuildSegmentDescription& segmentDescription) override { return true; }
    size_t GetLastConsumedMessageCount() const override { return 0; }

    size_t EstimateInitMemoryUse(const TableWriterInitParamPtr& initParam) const override { return 0; }

    size_t GetCurrentMemoryUse() const override { return 0; }
    size_t EstimateDumpMemoryUse() const override { return 0; }
    size_t EstimateDumpFileSize() const override { return 0; }
};
}; // namespace

void DumpSegmentQueueTest::TestGetBuildingSegmentReaders()
{
    index_base::SegmentInfoPtr segInfo1(new index_base::SegmentInfo());
    segInfo1->timestamp = 10;
    index_base::SegmentInfoPtr segInfo2(new index_base::SegmentInfo());
    segInfo2->timestamp = 100;
    TableWriterPtr tableWriter1(new UTTableWriter());
    TableWriterPtr tableWriter2(new UTTableWriter());

    std::string testPartName1("test1");
    std::string testPartName2("test2");
    MockCustomSegmentDumpItem* mockItem1 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName1);
    MockCustomSegmentDumpItem* mockItem2 = new MockCustomSegmentDumpItem(mOptions, mSchema, testPartName2);

    mockItem1->mTableWriter = tableWriter1;
    mockItem2->mTableWriter = tableWriter2;

    // InitSegmentItem(mockItem1, tableWriter);
    // InitSegmentItem(mockItem2, tableWriter);

    EXPECT_CALL(*mockItem1, GetSegmentInfo()).WillRepeatedly(Return(segInfo1));
    EXPECT_CALL(*mockItem2, GetSegmentInfo()).WillRepeatedly(Return(segInfo2));

    DumpSegmentQueuePtr queue = DumpSegmentQueuePtr(new DumpSegmentQueue);
    ASSERT_EQ(queue->Size(), 0);

    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem1));
    queue->PushDumpItem(CustomSegmentDumpItemPtr(mockItem2));

    vector<table::BuildingSegmentReaderPtr> dumpingSegmentReaders;

    // reclaimTimestamp < 0, skip
    queue->GetBuildingSegmentReaders(dumpingSegmentReaders, -1);
    ASSERT_EQ(2, queue->Size());
    ASSERT_EQ(2, dumpingSegmentReaders.size());

    // reclaimTimestamp > 0, but no invalid
    dumpingSegmentReaders.clear();
    queue->GetBuildingSegmentReaders(dumpingSegmentReaders, 1);
    ASSERT_EQ(2, queue->Size());
    ASSERT_EQ(2, dumpingSegmentReaders.size());

    // reclaimTimestamp > 0, first is filter
    dumpingSegmentReaders.clear();
    queue->GetBuildingSegmentReaders(dumpingSegmentReaders, 20);
    ASSERT_EQ(2, queue->Size());
    ASSERT_EQ(1, dumpingSegmentReaders.size());

    // reclaimTimestamp > 0, both is filter
    dumpingSegmentReaders.clear();
    queue->GetBuildingSegmentReaders(dumpingSegmentReaders, 200);
    ASSERT_EQ(2, queue->Size());
    ASSERT_EQ(0, dumpingSegmentReaders.size());
}
}} // namespace indexlib::partition
