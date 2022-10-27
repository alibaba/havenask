#include "indexlib/index/normal/deletionmap/test/deletion_map_writer_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(test);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DeletionMapWriterTest);

DeletionMapWriterTest::DeletionMapWriterTest()
{
}

DeletionMapWriterTest::~DeletionMapWriterTest()
{
}

void DeletionMapWriterTest::CaseSetUp()
{
}

void DeletionMapWriterTest::CaseTearDown()
{
}

void DeletionMapWriterTest::TestDeleteWithCachePool()
{
    //test delete by patch
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,6,6;1,1,7");
    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());
    
    DeletionMapWriter writer(true);
    writer.Init(partitionData.get());
    writer.Delete(0);

    DeletionMapReader reader;
    reader.Open(partitionData.get());

    ASSERT_FALSE(reader.IsDeleted(0));
    DirectoryPtr rootDirectory = GET_PARTITION_DIRECTORY();
    DirectoryPtr segmentDirectory = rootDirectory->MakeDirectory("segment_2_level_0");
    SegmentInfo segmentInfo;
    segmentInfo.Store(segmentDirectory);
    
    writer.Dump(segmentDirectory);
    Version version = partitionData->GetVersion();
    version.AddSegment(2);
    version.IncVersionId();
    version.Store(rootDirectory, false);

    partitionData = 
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());
    reader.Open(partitionData.get());
    ASSERT_TRUE(reader.IsDeleted(0));
}

void DeletionMapWriterTest::TestDeleteWithoutCachePool()
{
    //test delete inplace
    PartitionDataMaker::MakePartitionDataFiles(
            0, 0, GET_PARTITION_DIRECTORY(), "0,6,6;1,1,7");
    PartitionDataPtr partitionData = 
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());
    
    DeletionMapWriter writer(false);
    writer.Init(partitionData.get());
    writer.Delete(0);

    DeletionMapReader reader;
    reader.Open(partitionData.get());
    ASSERT_TRUE(reader.IsDeleted(0));
}

void DeletionMapWriterTest::TestCopyOnDumpForAsyncFlushOperation()
{
    LoadConfigList loadConfigList;
    RESET_FILE_SYSTEM(loadConfigList, true, true, true);
    
    DeletionMapSegmentWriter segWriter;
    segWriter.Init(10);
    segWriter.Delete(5);

    DirectoryPtr segDir = GET_SEGMENT_DIRECTORY();
    segWriter.Dump(segDir, 0);
    {
        // shared file node from cache
        DeletionMapSegmentWriter newWriter;
        newWriter.Init(segDir, "data_0", false);
        newWriter.Delete(4);
    }

    // trigger dump
    segDir->Sync(true);
    segDir->GetFileSystem()->CleanCache();
    {
        // check delete status
        DeletionMapSegmentWriter checkWriter;
        checkWriter.Init(segDir, "data_0", false);
        ASSERT_EQ(1, checkWriter.GetDeletedCount());
    }
}

IE_NAMESPACE_END(index);

