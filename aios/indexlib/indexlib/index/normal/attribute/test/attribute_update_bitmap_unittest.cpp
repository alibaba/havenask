#include "indexlib/index/normal/attribute/test/attribute_update_bitmap_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"

using namespace std;
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeUpdateBitmapTest);

AttributeUpdateBitmapTest::AttributeUpdateBitmapTest()
{
}

AttributeUpdateBitmapTest::~AttributeUpdateBitmapTest()
{
}

void AttributeUpdateBitmapTest::CaseSetUp()
{
}

void AttributeUpdateBitmapTest::CaseTearDown()
{
}

void AttributeUpdateBitmapTest::TestSimpleProcess()
{
    string segInfoStr = "3,30,0;4,20,10;7,100,9";
    PartitionDataMaker::MakePartitionDataFiles(
        versionid_t(1), 0, GET_PARTITION_DIRECTORY(), segInfoStr);

    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());

    AttributeUpdateBitmap attrBitmap;
    attrBitmap.Init(partitionData);

    // set 5 docs in segment 3
    attrBitmap.Set(25);
    attrBitmap.Set(20);
    attrBitmap.Set(15);
    attrBitmap.Set(10);
    attrBitmap.Set(5);
    // set 2 docs in segment 4
    attrBitmap.Set(30);
    attrBitmap.Set(35);
    attrBitmap.Set(40);
    attrBitmap.Set(49);
    // set 1 docs in segment 7
    attrBitmap.Set(60);
    attrBitmap.Set(60);
    attrBitmap.Set(60);
    // test out of range set will not throw  exception 
    ASSERT_NO_THROW(attrBitmap.Set(150));

    DirectoryPtr attrDir = GET_SEGMENT_DIRECTORY();
    attrBitmap.Dump(attrDir);

    AttributeUpdateInfo updateInfo;
    updateInfo.Load(attrDir);
    ASSERT_EQ(size_t(3), updateInfo.Size());

    SegmentUpdateInfo segmentUpdateInfo;
    AttributeUpdateInfo::Iterator iter = updateInfo.CreateIterator();
    ASSERT_TRUE(iter.HasNext());
    segmentUpdateInfo = iter.Next();
    ASSERT_EQ(segmentid_t(3), segmentUpdateInfo.updateSegId);
    ASSERT_EQ(uint32_t(5), segmentUpdateInfo.updateDocCount);
    
    ASSERT_TRUE(iter.HasNext());
    segmentUpdateInfo = iter.Next();
    ASSERT_EQ(segmentid_t(4), segmentUpdateInfo.updateSegId);
    ASSERT_EQ(uint32_t(4), segmentUpdateInfo.updateDocCount);
    
    ASSERT_TRUE(iter.HasNext());
    segmentUpdateInfo = iter.Next();
    ASSERT_EQ(segmentid_t(7), segmentUpdateInfo.updateSegId);
    ASSERT_EQ(uint32_t(1), segmentUpdateInfo.updateDocCount);
    ASSERT_FALSE(iter.HasNext());
}

IE_NAMESPACE_END(index);

