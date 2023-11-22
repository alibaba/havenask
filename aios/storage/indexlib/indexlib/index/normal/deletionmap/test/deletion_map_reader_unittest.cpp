#include "indexlib/index/normal/deletionmap/test/deletion_map_reader_unittest.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/util/dump_item_typed.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_define.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;

using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::common;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, DeletionMapReaderTest);

DeletionMapReaderTest::DeletionMapReaderTest() {}

DeletionMapReaderTest::~DeletionMapReaderTest() {}

void DeletionMapReaderTest::CaseSetUp() { mRootDir = GET_TEMP_DATA_PATH(); }

void DeletionMapReaderTest::CaseTearDown() {}

void DeletionMapReaderTest::TestCaseForReadAndWrite()
{
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());

    InnerTestDeletionmapReadAndWrite(6, "1,5", "1,5", partitionData);
    InnerTestDeletionmapReadAndWrite(1, "2", "1,5,2", partitionData);

    // test docId 11, test over-bound doc of building segment
    InnerTestDeletionmapReadAndWrite(4, "8,1", "1,5,2,8,11", partitionData);

    // delete doc 6, test delete doc without deletionmap file
    InnerTestDeletionmapReadAndWrite(5, "4,6,3", "1,5,2,8,4,6,3", partitionData);
    InnerTestDeletionmapReadAndWrite(3, "16,2,6", "1,5,2,8,4,6,3,16", partitionData);
}

// for bug #242437
void DeletionMapReaderTest::TestCaseForGetDeletedDocCount()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,0,0;1,10,10;2,10,20");
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());

    DeletionMapReader reader;
    reader.Open(partitionData.get());
    reader.Delete(1, 5);
    reader.Delete(2, 5);

    INDEXLIB_TEST_EQUAL((uint32_t)0, reader.GetDeletedDocCount(0));
    INDEXLIB_TEST_EQUAL((uint32_t)1, reader.GetDeletedDocCount(1));
    INDEXLIB_TEST_EQUAL((uint32_t)1, reader.GetDeletedDocCount(2));
}

void DeletionMapReaderTest::TestCaseForReaderDelete()
{
    PartitionDataMaker::MakePartitionDataFiles(0, 0, GET_PARTITION_DIRECTORY(), "0,6,6;1,1,7");
    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM());

    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    SegmentInfoPtr buildingSegmentInfo = inMemSegment->GetSegmentInfo();
    buildingSegmentInfo->docCount = 5;

    DeletionMapReader reader;
    reader.Open(partitionData.get());

    reader.Delete(1);
    reader.Delete(5);
    reader.Delete(2);
    reader.Delete(10);

    INDEXLIB_TEST_TRUE(reader.IsDeleted(10));
    INDEXLIB_TEST_TRUE(!reader.IsDeleted(11));
    INDEXLIB_TEST_TRUE(reader.IsDeleted(12));
    INDEXLIB_TEST_EQUAL((uint32_t)4, reader.GetDeletedDocCount());
}

void DeletionMapReaderTest::TestCaseForMultiInMemorySegments()
{
    IndexPartitionOptions options;
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &options);
    string field = "pk:uint64:pk;long1:uint32";
    string index = "pk:primarykey64:pk";
    config::IndexPartitionSchemaPtr schema = SchemaMaker::MakeSchema(field, index, "long1", "");

    FakePartitionDataCreator pdc;
    pdc.Init(schema, options, GET_TEMP_DATA_PATH());

    string fullDocs = "cmd=add,pk=0,long1=0;"
                      "cmd=add,pk=1,long1=1;";
    pdc.AddFullDocs(fullDocs);

    string builtRtDocs = "cmd=add,pk=2,long1=2,ts=0;"
                         "cmd=add,pk=3,long1=3,ts=0;";
    pdc.AddBuiltSegment(builtRtDocs);

    string inMemSegment1 = "cmd=add,pk=4,long1=4,ts=0;"
                           "cmd=add,pk=5,long1=5,ts=0;";
    pdc.AddInMemSegment(inMemSegment1);

    // delete built & building segments
    string inMemSegment2 = "cmd=add,pk=6,long1=6,ts=0;"
                           "cmd=add,pk=7,long1=7,ts=0;"
                           "cmd=add,pk=8,long1=8,ts=0;"
                           "cmd=delete,pk=2,ts=0;"
                           "cmd=delete,pk=5,ts=0;"
                           "cmd=delete,pk=6,ts=0;";
    pdc.AddInMemSegment(inMemSegment2);

    string buildingDocs = "cmd=delete,pk=4,ts=0;"
                          "cmd=delete,pk=7,ts=0;";
    pdc.AddBuildingData(buildingDocs);

    // check before dump
    ASSERT_EQ((size_t)2, pdc.GetDumpContainerSize());
    CheckResult(pdc.CreatePartitionData(false),
                "0:true;1:true;2:false;3:true;4:false;5:false;6:false;7:false;8:true;9:false");

    pdc.DisableSlowDump();
    pdc.WaitDumpContainerEmpty();
    pdc.Reopen();

    // check after dump
    ASSERT_EQ((size_t)0, pdc.GetDumpContainerSize());
    CheckResult(pdc.CreatePartitionData(false),
                "0:true;1:true;2:false;3:true;4:false;5:false;6:false;7:false;8:true;9:false");
}

void DeletionMapReaderTest::InnerTestDeletionmapReadAndWrite(uint32_t segmentDocCount, const string& toDeleteDocs,
                                                             const string& deletedDocs,
                                                             const PartitionDataPtr& partitionData)
{
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    SegmentInfoPtr segmentInfo = inMemSegment->GetSegmentInfo();
    segmentInfo->docCount = segmentDocCount;
    DeletionMapWriter writer(false);
    writer.Init(partitionData.get());

    vector<docid_t> toDeleteDocIds;
    StringUtil::fromString(toDeleteDocs, toDeleteDocIds, ",");
    for (size_t i = 0; i < toDeleteDocIds.size(); i++) {
        writer.Delete(toDeleteDocIds[i]);
    }

    // test read in building mode
    CheckDeletedDocs(deletedDocs, partitionData);

    DirectoryPtr directory = inMemSegment->GetDirectory();
    writer.Dump(directory);
    SegmentWriterPtr inMemSegmentWriter = inMemSegment->GetSegmentWriter();
    vector<std::unique_ptr<common::DumpItem>> dumpItems;
    inMemSegmentWriter->CreateDumpItems(directory, dumpItems);
    for (size_t i = 0; i < dumpItems.size(); ++i) {
        DumpItem* workItem = dumpItems[i].release();
        workItem->process(NULL);
        workItem->destroy();
    }
    segmentInfo->Store(directory);
    inMemSegment->SetStatus(InMemorySegment::DUMPED);
    partitionData->AddBuiltSegment(inMemSegment->GetSegmentId(), segmentInfo);
    partitionData->CreateNewSegment();

    // test read in built mode
    CheckDeletedDocs(deletedDocs, partitionData);
}

void DeletionMapReaderTest::CheckDeletedDocs(const string& deletedDocs, const PartitionDataPtr& partitionData)
{
    vector<docid_t> deletedDocIds;
    StringUtil::fromString(deletedDocs, deletedDocIds, ",");
    DeletionMapReaderPtr reader(new DeletionMapReader);
    reader->Open(partitionData.get());
    for (size_t i = 0; i < deletedDocIds.size(); i++) {
        ASSERT_TRUE(reader->IsDeleted(deletedDocIds[i]));
    }
}

void DeletionMapReaderTest::CheckResult(const PartitionDataPtr& partitionData, const string& resultStr)
{
    DeletionMapReader reader;
    ASSERT_TRUE(reader.Open(partitionData.get()));

    vector<vector<string>> resultInfos;
    StringUtil::fromString(resultStr, resultInfos, ":", ";");

    for (size_t i = 0; i < resultInfos.size(); i++) {
        assert(resultInfos[i].size() == 2);
        bool isDeleted = (resultInfos[i][1] == "false");
        docid_t docId = StringUtil::fromString<docid_t>(resultInfos[i][0]);
        ASSERT_EQ(isDeleted, reader.IsDeleted(docId)) << docId;
    }
}
}} // namespace indexlib::index
