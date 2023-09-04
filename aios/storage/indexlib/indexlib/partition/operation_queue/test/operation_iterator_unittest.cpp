#include "indexlib/common_define.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {
class OperationIteratorTest : public INDEXLIB_TESTBASE_WITH_PARAM<bool>
{
public:
    OperationIteratorTest();
    ~OperationIteratorTest();

    DECLARE_CLASS_NAME(OperationIteratorTest);

private:
    config::IndexPartitionSchemaPtr mSchema;

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    void InnerTestIterateOperations(const std::string& segInfoStr, size_t buildingOpCount, size_t opBlockSize,
                                    bool compressOperation, const OperationCursor& startCursor, int64_t skipTs,
                                    size_t expectOpCount, int64_t expectStartTs,
                                    const OperationCursor& expectLastCursor, bool hasDumpingSegment = false);

    OperationWriterPtr CreateOperationWriter(const config::IndexPartitionSchemaPtr& schema, size_t operationBlockSize,
                                             bool compressOperation);

    // eg. version [builtSeg1, builtSeg2, buildingSeg1...], so the builtBuildingSplitCursor is 2
    index_base::PartitionDataPtr PreparePartitionData(const std::string& segInfoStr, size_t buildingOpCount,
                                                      size_t operationBlockSize, bool compressOperation,
                                                      bool hasDumpingSegment, const std::string& opTimeStampStr,
                                                      uint32_t segmentStartTs, size_t builtBuildingSplitCursor,
                                                      bool isOnline);

    void GetTimestampVectorForTargetSegments(const std::vector<int32_t>& segOpCounts, size_t buildingOpCount,
                                             const std::string& opTimeStampStr,
                                             std::vector<std::vector<uint32_t>>& segOpTsVec,
                                             std::vector<uint32_t>& buildingOpTsVec);
};

INSTANTIATE_TEST_CASE_P(enableFlushToDisk, OperationIteratorTest, testing::Values(false, true));

OperationIteratorTest::OperationIteratorTest() {}

OperationIteratorTest::~OperationIteratorTest() {}

void OperationIteratorTest::CaseSetUp()
{
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void OperationIteratorTest::CaseTearDown() {}

TEST_P(OperationIteratorTest, TestSimpleProcess)
{
    OperationCursor startCursor(1, 0);
    OperationCursor lastCursor(3, 4);
    InnerTestIterateOperations("3,2,4", 5, 2, false, startCursor, 4, 10, 4, lastCursor);
}

TEST_P(OperationIteratorTest, TestIterateOperations)
{
    typedef OperationCursor Cursor;
    // test different block size & load from built segment
    InnerTestIterateOperations("3,2,4", 5, 1, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
    InnerTestIterateOperations("3,2,4", 5, 1, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
    InnerTestIterateOperations("3,2,4", 5, 2, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
    InnerTestIterateOperations("3,2,4", 5, 2, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
    InnerTestIterateOperations("3,2,4", 5, 6, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4));
}

TEST_P(OperationIteratorTest, TestIterateOperations_LoadFromLastCursor)
{
    typedef OperationCursor Cursor;
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(3, 4), 4, 0, 5, Cursor(3, 4));
}

TEST_P(OperationIteratorTest, TestIterateOperations_LoadFromInvalidTimestamp)
{
    typedef OperationCursor Cursor;
    // test load from invalid timestamp
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1, 0), 14, 0, 0, Cursor(3, 4));
}

TEST_P(OperationIteratorTest, TestIterateOperations_LoadBuildingSegment)
{
    typedef OperationCursor Cursor;
    // test start load cursor in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(3, 0), 12, 7, 12, Cursor(3, 9));
    // test start load cursor in building segment, built block
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(3, 0), 12, 7, 12, Cursor(3, 9));
    // test start load cursor in bulding segment, building block
    InnerTestIterateOperations("3,2,4", 11, 3, true, Cursor(3, 0), 18, 2, 18, Cursor(3, 10));

    // test skip timetamp in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(1, 0), 12, 7, 12, Cursor(3, 9));
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(1, 0), 12, 7, 12, Cursor(3, 9));
}

TEST_P(OperationIteratorTest, TestIterateOperations_LoadDumpingSegment)

{
    typedef OperationCursor Cursor;
    // ******************** with one dumping segment *****************************
    // const string &segInfoStr, size_t buildingOpCount, size_t opBlockSize, bool compressOperation,
    //     const OperationCursor &startCursor, int64_t skipTs, size_t expectOpCount, int64_t expectStartTs,
    //     const OperationCursor &expectLastCursor,
    //     bool hasDumpingSegment

    // test different block size & load from built segment
    InnerTestIterateOperations("3,2,4", 5, 1, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);
    InnerTestIterateOperations("3,2,4", 5, 1, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);
    InnerTestIterateOperations("3,2,4", 5, 2, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);
    InnerTestIterateOperations("3,2,4", 5, 2, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);
    InnerTestIterateOperations("3,2,4", 5, 6, false, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1, 0), 4, 10, 4, Cursor(3, 4), true);

    // test load from last cursor
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(3, 4), 4, 0, 5, Cursor(3, 4), true);
    // test load from invalid timestamp
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1, 0), 14, 0, 0, Cursor(3, 4), true);

    // test start load cursor in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(3, 0), 12, 7, 12, Cursor(3, 9), true);
    // test start load cursor in building segment, built block
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(3, 0), 12, 7, 12, Cursor(3, 9), true);
    // test start load cursor in bulding segment, building block
    InnerTestIterateOperations("3,2,4", 11, 3, true, Cursor(3, 0), 18, 2, 18, Cursor(3, 10), true);

    // test skip timetamp in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(1, 0), 12, 7, 12, Cursor(3, 9), true);
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(1, 0), 12, 7, 12, Cursor(3, 9), true);
}

TEST_P(OperationIteratorTest, TestRedoBreakAtWholeObseleteOperationSegment_20681011)
{
    {
        // obselete segment in middle
        PartitionDataPtr partitionData =
            PreparePartitionData("3,2,4", 3, 2, true, false, "10,11,12,2,3,14,9,15,16,17,18,19",
                                 /*segmentStartTs=*/0, /*builtBuildingSplitCursor=*/-1, /*isOnline=*/false);

        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 1, ts=2,3 < 10
        uint32_t expectTsVec[] = {10, 11, 12, 14, 15, 16, 17, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++) {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3, 2), lastCursor);
    }

    {
        tearDown();
        setUp();
        // obselete segment in first position
        PartitionDataPtr partitionData =
            PreparePartitionData("3,2,4", 3, 2, true, false, "2,3,4,10,11,14,9,15,16,17,18,19", 10,
                                 /*builtBuildingSplitCursor=*/-1, /*isOnline=*/false);

        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 0, segmentTs = 10, ts=2,3,4 < 10
        uint32_t expectTsVec[] = {10, 11, 14, 15, 16, 17, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++) {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3, 2), lastCursor);
    }

    {
        tearDown();
        setUp();
        // obselete segment in last position
        PartitionDataPtr partitionData =
            PreparePartitionData("3,2,4", 3, 2, true, false, "10,11,12,2,12,14,9,8,7,6,18,19", /*segmentStartTs=*/0,
                                 /*builtBuildingSplitCursor=*/-1, /*isOnline=*/false);

        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 2, segmentTs = 14, ts=9,8,7,6 < 10
        uint32_t expectTsVec[] = {10, 11, 12, 12, 14, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++) {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3, 2), lastCursor);
    }
}

TEST_P(OperationIteratorTest, TestOpenGetTotalBuildingOperationCntFromCursor)
{
    {
        TearDown();
        SetUp();
        size_t operationBlockSize = 2;
        // obselete segment in first position
        PartitionDataPtr partData =
            PreparePartitionData("3,2,4", 3, operationBlockSize, true, true, "2,3,4,9,10,11,14,15,16,17,18,19", 0,
                                 /*builtBuildingSplitCursor=*/0, /*isOnline=*/true);

        auto cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, OperationCursor());
        ASSERT_EQ(12, cnt);

        {
            OperationCursor cursor(1073741824);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(12, cnt);
        }
        {
            OperationCursor cursor(1073741824, 0);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(12, cnt);
        }
        {
            OperationCursor cursor(1073741824, 1);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(11, cnt);
        }
        {
            OperationCursor cursor(1073741824, 2);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(10, cnt);
        }
        {
            OperationCursor cursor(1073741824, 3);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(9, cnt);
        }
        {
            OperationCursor cursor(1073741825);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(9, cnt);
        }
        {
            OperationCursor cursor(1073741825, 0);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(9, cnt);
        }
        {
            OperationCursor cursor(1073741827);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(3, cnt);
        }
        {
            OperationCursor cursor(1073741827, 0);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(3, cnt);
        }
        {
            OperationCursor cursor(1073741827, 1);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(2, cnt);
        }
        {
            OperationCursor cursor(1073741827, 2);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(1, cnt);
        }
        {
            OperationCursor cursor(1073741827, 3);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(0, cnt);
        }

        // test catch up scene. add another 2 op to current building segment
        OperationIterator iter(partData, mSchema);
        iter.Init(0, OperationCursor());

        size_t opCount = 15;
        InMemorySegmentPtr inMemSegment = iter.mBuildingSegment;
        OperationWriterPtr opWriter;
        opWriter = DYNAMIC_POINTER_CAST(OperationWriter, inMemSegment->GetOperationWriter());
        for (size_t i = 0; i < 2; ++i) {
            stringstream docStr;
            docStr << "cmd=update_field,string1=pkStr,price=" << opCount << ",ts=" << opCount + i;
            ++opCount;
            NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr.str());
            opWriter->AddOperation(doc);
        }
        {
            OperationCursor cursor(1073741827);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(5, cnt);
        }
        {
            OperationCursor cursor(1073741827, 3);
            cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
            ASSERT_EQ(2, cnt);
        }

        // // add new waiting_to_dump segment to DumpSegmentQueue
        // OperationWriterPtr opNewWriter = CreateOperationWriter(
        //         mSchema, operationBlockSize, true);
        // for (size_t i = 0; i < 2; ++i) {
        //     stringstream docStr;
        //     docStr << "cmd=update_field,string1=pkStr,price="
        //            << opCount << ",ts=" << opCount + i;
        //     ++opCount;
        //     NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr.str());
        //     opNewWriter->AddOperation(doc);
        // }
        // inMemSegment->SetStatus(InMemorySegment::WAITING_TO_DUMP);
        // auto segId = inMemSegment->GetSegmentId();
        // inMemSegment = fakePartData.mPartData->CreateNewSegment();
        // inMemSegment->mSegmentData.SetSegmentId(++segId);
        // inMemSegment->SetOperationWriter(opNewWriter);

        // {
        //     OperationCursor cursor(1073741827);
        //     cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
        //     ASSERT_EQ(7, cnt);
        // }
        // {
        //     OperationCursor cursor(1073741827, 3);
        //     cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
        //     ASSERT_EQ(4, cnt);
        // }
        // {
        //     OperationCursor cursor(1073741828);
        //     cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
        //     ASSERT_EQ(2, cnt);
        // }
        // {
        //     OperationCursor cursor(1073741828, 0);
        //     cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
        //     ASSERT_EQ(2, cnt);
        // }
        // {
        //     OperationCursor cursor(1073741828, 1);
        //     cnt = OperationIterator::GetTotalBuildingOperationCntFromCursor(partData, cursor);
        //     ASSERT_EQ(1, cnt);
        // }
    }
}

void OperationIteratorTest::InnerTestIterateOperations(const string& segInfoStr, size_t buildingOpCount,
                                                       size_t opBlockSize, bool compressOperation,
                                                       const OperationCursor& startCursor, int64_t skipTs,
                                                       size_t expectOpCount, int64_t expectStartTs,
                                                       const OperationCursor& expectLastCursor, bool hasDumpingSegment)
{
    tearDown();
    setUp();
    // build
    PartitionDataPtr partitionData =
        PreparePartitionData(segInfoStr, buildingOpCount, opBlockSize, compressOperation, hasDumpingSegment,
                             /*opTimeStampStr=*/"", /*segmentStartTs=*/0,
                             /*builtBuildingSplitCursor=*/-1, /*isOnline=*/false);

    OperationIterator iter(partitionData, mSchema);
    iter.Init(skipTs, startCursor);

    int64_t expectTs = expectStartTs;
    for (size_t i = 0; i < expectOpCount; i++) {
        ASSERT_TRUE(iter.HasNext());
        OperationBase* op = iter.Next();
        ASSERT_TRUE(op);
        ASSERT_EQ(expectTs++, op->GetTimestamp());
    }
    ASSERT_FALSE(iter.HasNext());
    OperationCursor lastCursor = iter.GetLastCursor();
    EXPECT_EQ(expectLastCursor.pos, lastCursor.pos);
    EXPECT_EQ(expectLastCursor.segId, lastCursor.segId);
}

OperationWriterPtr OperationIteratorTest::CreateOperationWriter(const IndexPartitionSchemaPtr& schema,
                                                                size_t operationBlockSize, bool compressOperation)
{
    OperationWriterPtr opWriter;
    if (compressOperation) {
        opWriter.reset(new CompressOperationWriter());
    } else {
        opWriter.reset(new OperationWriter());
    }
    opWriter->Init(schema, operationBlockSize, GetParam());
    opWriter->SetReleaseBlockAfterDump(false);
    return opWriter;
}

PartitionDataPtr OperationIteratorTest::PreparePartitionData(const string& segInfoStr, size_t buildingOpCount,
                                                             size_t operationBlockSize, bool compressOperation,
                                                             bool hasDumpingSegment, const std::string& opTimeStampStr,
                                                             uint32_t segmentStartTs, size_t builtBuildingSplitCursor,
                                                             bool isOnline)
{
    IndexPartitionOptions options;
    options.SetIsOnline(isOnline);

    vector<SegmentData> builtSegments;
    vector<InMemorySegmentPtr> dumpingSegments;
    InMemorySegmentPtr buildingSegment;

    vector<int32_t> operationCounts;
    StringUtil::fromString(segInfoStr, operationCounts, ",");

    vector<vector<uint32_t>> segOpTsVec;
    vector<uint32_t> buildingOpTsVec;

    GetTimestampVectorForTargetSegments(operationCounts, buildingOpCount, opTimeStampStr, segOpTsVec, buildingOpTsVec);

    PartitionDataPtr partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    uint32_t opCount = 0;
    uint32_t maxTs = segmentStartTs;
    segmentid_t rtSegId = 1073741824;
    versionid_t vid = 1;
    Version version(vid);
    for (size_t i = 0; i < operationCounts.size(); i++) {
        SegmentInfo segmentInfo;
        OperationWriterPtr opWriter = CreateOperationWriter(mSchema, operationBlockSize, compressOperation);
        for (int32_t j = 0; j < operationCounts[i]; ++j) {
            stringstream docStr;
            docStr << "cmd=update_field,string1=pkStr,price=" << opCount << ",ts=" << segOpTsVec[i][j];
            maxTs = max(maxTs, segOpTsVec[i][j]);
            segmentInfo.timestamp = maxTs;
            ++opCount;
            NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr.str());
            opWriter->AddOperation(doc);
        }

        const bool isBuildingSegment = (i >= builtBuildingSplitCursor);
        const bool isDumpingSegment = hasDumpingSegment && (isBuildingSegment || i == operationCounts.size() - 1);
        if (isDumpingSegment) {
            // last segment is dumping segment
            Version tmpVersion;
            tmpVersion = version;
            tmpVersion.SetVersionId(++vid);
            tmpVersion.Store(GET_PARTITION_DIRECTORY(), false);
            partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
            for (size_t idx = 0; idx < tmpVersion.GetSegmentCount() && !isBuildingSegment; idx++) {
                builtSegments.push_back(partitionData->GetSegmentData(tmpVersion[idx]));
            }
            InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
            inMemSegment->SetOperationWriter(opWriter);
            *inMemSegment->GetSegmentWriter()->GetSegmentInfo() = segmentInfo;
            inMemSegment->SetStatus(InMemorySegment::WAITING_TO_DUMP);
            if (isBuildingSegment) {
                inMemSegment->mSegmentData.SetSegmentId(rtSegId++);
            }
            dumpingSegments.push_back(inMemSegment);
        }
        if (!isBuildingSegment) {
            version.AddSegment(i);
            string segName = version.GetSegmentDirName(i);
            DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(segName);
            assert(segDirectory);
            opWriter->Dump(segDirectory);
            segmentInfo.Store(segDirectory);
        }
    }
    version.Store(GET_PARTITION_DIRECTORY(), false);
    partitionData = PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    if (builtSegments.empty() && builtBuildingSplitCursor == -1) {
        for (size_t idx = 0; idx < version.GetSegmentCount(); idx++) {
            builtSegments.push_back(partitionData->GetSegmentData(version[idx]));
        }
    }

    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
    if (builtBuildingSplitCursor != -1) {
        inMemSegment->mSegmentData.SetSegmentId(rtSegId);
    }
    OperationWriterPtr opWriter = CreateOperationWriter(mSchema, operationBlockSize, compressOperation);
    for (size_t i = 0; i < buildingOpCount; ++i) {
        stringstream docStr;
        docStr << "cmd=update_field,string1=pkStr,price=" << opCount << ",ts=" << buildingOpTsVec[i];
        maxTs = max(maxTs, buildingOpTsVec[i]);
        ++opCount;
        NormalDocumentPtr doc = DocumentCreator::CreateNormalDocument(mSchema, docStr.str());
        opWriter->AddOperation(doc);
    }
    inMemSegment->SetOperationWriter(opWriter);
    buildingSegment = inMemSegment;

    FakePartitionDataPtr fakePartData(new FakePartitionData(partitionData));
    fakePartData->Init(builtSegments, dumpingSegments, buildingSegment);
    return fakePartData;
}

void OperationIteratorTest::GetTimestampVectorForTargetSegments(const vector<int32_t>& segOpCounts,
                                                                size_t buildingOpCount, const string& opTimeStampStr,
                                                                vector<vector<uint32_t>>& segOpTsVec,
                                                                vector<uint32_t>& buildingOpTsVec)
{
    vector<uint32_t> opTsVec;
    StringUtil::fromString(opTimeStampStr, opTsVec, ",");
    uint32_t totalOpCount = 0;
    for (size_t i = 0; i < segOpCounts.size(); i++) {
        vector<uint32_t> tsVec;
        for (int32_t j = 0; j < segOpCounts[i]; j++) {
            uint32_t ts = totalOpCount;
            if (!opTsVec.empty()) {
                assert(totalOpCount < (uint32_t)opTsVec.size());
                ts = opTsVec[totalOpCount];
            }
            tsVec.push_back(ts);
            totalOpCount++;
        }
        segOpTsVec.push_back(tsVec);
    }
    assert(buildingOpTsVec.empty());
    for (size_t i = 0; i < buildingOpCount; i++) {
        uint32_t ts = totalOpCount;
        if (!opTsVec.empty()) {
            assert(totalOpCount < (uint32_t)opTsVec.size());
            ts = opTsVec[totalOpCount];
        }
        buildingOpTsVec.push_back(ts);
        totalOpCount++;
    }

    assert(opTsVec.size() == 0 || opTsVec.size() == totalOpCount);
}
}} // namespace indexlib::partition
