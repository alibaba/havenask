#include "indexlib/partition/operation_queue/test/operation_iterator_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/fake_partition_data_creator.h"
#include "indexlib/partition/operation_queue/compress_operation_writer.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/test/document_creator.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationIteratorTest);

OperationIteratorTest::OperationIteratorTest()
{
}

OperationIteratorTest::~OperationIteratorTest()
{
}

void OperationIteratorTest::CaseSetUp()
{
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void OperationIteratorTest::CaseTearDown()
{
}

void OperationIteratorTest::TestSimpleProcess()
{
    OperationCursor startCursor(1, 0);
    OperationCursor lastCursor(3, 4);
    InnerTestIterateOperations("3,2,4", 5, 2, false, startCursor, 4, 10, 4, lastCursor);
}

void OperationIteratorTest::TestIterateOperations()
{
    typedef OperationCursor Cursor;
    // test different block size & load from built segment
    InnerTestIterateOperations("3,2,4", 5, 1, false, Cursor(1,0), 4, 10, 4, Cursor(3,4));
    InnerTestIterateOperations("3,2,4", 5, 1, true, Cursor(1,0), 4, 10, 4, Cursor(3,4));
    InnerTestIterateOperations("3,2,4", 5, 2, false, Cursor(1,0), 4, 10, 4, Cursor(3,4));
    InnerTestIterateOperations("3,2,4", 5, 2, true, Cursor(1,0), 4, 10, 4, Cursor(3,4));
    InnerTestIterateOperations("3,2,4", 5, 6, false, Cursor(1,0), 4, 10, 4, Cursor(3,4));
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1,0), 4, 10, 4, Cursor(3,4));

    // test load from last cursor
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(3,4), 4, 0, 5, Cursor(3,4));
    // test load from invalid timestamp
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1,0), 14, 0, 0, Cursor(3,4)); 

    // test start load cursor in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(3,0), 12, 7, 12, Cursor(3,9));
    // test start load cursor in building segment, built block
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(3,0), 12, 7, 12, Cursor(3,9));
    // test start load cursor in bulding segment, building block
    InnerTestIterateOperations("3,2,4", 11, 3, true, Cursor(3,0), 18, 2, 18, Cursor(3,10));

    // test skip timetamp in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(1,0), 12, 7, 12, Cursor(3,9));
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(1,0), 12, 7, 12, Cursor(3,9));

    // ******************** with one dumping segment ***************************** 

    // test different block size & load from built segment
    InnerTestIterateOperations("3,2,4", 5, 1, false, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);
    InnerTestIterateOperations("3,2,4", 5, 1, true, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);
    InnerTestIterateOperations("3,2,4", 5, 2, false, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);
    InnerTestIterateOperations("3,2,4", 5, 2, true, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);
    InnerTestIterateOperations("3,2,4", 5, 6, false, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1,0), 4, 10, 4, Cursor(3,4), true);

    // test load from last cursor
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(3,4), 4, 0, 5, Cursor(3,4), true);
    // test load from invalid timestamp
    InnerTestIterateOperations("3,2,4", 5, 6, true, Cursor(1,0), 14, 0, 0, Cursor(3,4), true); 

    // test start load cursor in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(3,0), 12, 7, 12, Cursor(3,9), true);
    // test start load cursor in building segment, built block
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(3,0), 12, 7, 12, Cursor(3,9), true);
    // test start load cursor in bulding segment, building block
    InnerTestIterateOperations("3,2,4", 11, 3, true, Cursor(3,0), 18, 2, 18, Cursor(3,10), true);

    // test skip timetamp in building segment
    InnerTestIterateOperations("3,2,4", 10, 3, false, Cursor(1,0), 12, 7, 12, Cursor(3,9), true);
    InnerTestIterateOperations("3,2,4", 10, 3, true, Cursor(1,0), 12, 7, 12, Cursor(3,9), true); 
}

void OperationIteratorTest::TestRedoBreakAtWholeObseleteOperationSegment_20681011()
{
    {
        // obselete segment in middle
        PartitionDataPtr partitionData = PreparePartitionData(
                "3,2,4", 3, 2, true, false, "10,11,12,2,3,14,9,15,16,17,18,19");
    
        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 1, ts=2,3 < 10
        uint32_t expectTsVec[] = {10, 11, 12, 14, 15, 16, 17, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++)
        {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3,2), lastCursor);
    }

    {
        TearDown();
        SetUp();
        // obselete segment in first position
        PartitionDataPtr partitionData = PreparePartitionData(
                "3,2,4", 3, 2, true, false, "2,3,4,10,11,14,9,15,16,17,18,19", 10);
    
        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 0, segmentTs = 10, ts=2,3,4 < 10
        uint32_t expectTsVec[] = {10, 11, 14, 15, 16, 17, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++)
        {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3,2), lastCursor);
    }

    {
        TearDown();
        SetUp();
        // obselete segment in last position
        PartitionDataPtr partitionData = PreparePartitionData(
                "3,2,4", 3, 2, true, false, "10,11,12,2,12,14,9,8,7,6,18,19");
    
        OperationIterator iter(partitionData, mSchema);
        iter.Init(10, OperationCursor());

        // segmentIdx = 2, segmentTs = 14, ts=9,8,7,6 < 10
        uint32_t expectTsVec[] = {10, 11, 12, 12, 14, 18, 19};
        for (size_t i = 0; i < sizeof(expectTsVec) / sizeof(uint32_t); i++)
        {
            ASSERT_TRUE(iter.HasNext()) << i;
            OperationBase* op = iter.Next();
            ASSERT_TRUE(op);
            ASSERT_EQ(expectTsVec[i], op->GetTimestamp());
        }
        ASSERT_FALSE(iter.HasNext());
        OperationCursor lastCursor = iter.GetLastCursor();
        ASSERT_EQ(OperationCursor(3,2), lastCursor);
    }

}

void OperationIteratorTest::InnerTestIterateOperations(
        const string& segInfoStr, size_t buildingOpCount,
        size_t opBlockSize, bool compressOperation,
        const OperationCursor& startCursor, int64_t skipTs,
        size_t expectOpCount, int64_t expectStartTs,
        const OperationCursor& expectLastCursor,
        bool hasDumpingSegment)
{
    TearDown();
    SetUp();
    // build
    PartitionDataPtr partitionData = PreparePartitionData(
            segInfoStr, buildingOpCount, opBlockSize,
            compressOperation, hasDumpingSegment);

    OperationIterator iter(partitionData, mSchema);
    iter.Init(skipTs, startCursor);

    int64_t expectTs = expectStartTs;
    for (size_t i = 0; i < expectOpCount; i++)
    {
        ASSERT_TRUE(iter.HasNext());
        OperationBase* op = iter.Next();
        ASSERT_TRUE(op);
        ASSERT_EQ(expectTs++, op->GetTimestamp());
    }
    ASSERT_FALSE(iter.HasNext());
    OperationCursor lastCursor = iter.GetLastCursor();
    ASSERT_EQ(expectLastCursor, lastCursor);
}

OperationWriterPtr OperationIteratorTest::CreateOperationWriter(
        const IndexPartitionSchemaPtr& schema, size_t operationBlockSize,
        bool compressOperation)
{
    OperationWriterPtr opWriter;
    if (compressOperation)
    {
        opWriter.reset(new CompressOperationWriter());
    }
    else
    {
        opWriter.reset(new OperationWriter());
    }
    opWriter->Init(schema, operationBlockSize);
    opWriter->SetReleaseBlockAfterDump(false);
    return opWriter;
}

PartitionDataPtr OperationIteratorTest::PreparePartitionData(
        const string& segInfoStr, size_t buildingOpCount,
        size_t operationBlockSize, bool compressOperation,
        bool hasDumpingSegment, const std::string& opTimeStampStr,
        uint32_t segmentStartTs)
{
    IndexPartitionOptions options;
    options.SetIsOnline(false);

    vector<SegmentData> builtSegments;
    vector<InMemorySegmentPtr> dumpingSegments;
    InMemorySegmentPtr buildingSegment;

    vector<int32_t> operationCounts;
    StringUtil::fromString(segInfoStr, operationCounts, ",");

    vector<vector<uint32_t> > segOpTsVec;
    vector<uint32_t> buildingOpTsVec;

    GetTimestampVectorForTargetSegments(
            operationCounts, buildingOpCount,
            opTimeStampStr, segOpTsVec, buildingOpTsVec);
    uint32_t opCount = 0;
    uint32_t maxTs = segmentStartTs;
    Version version(1);
    for (size_t i = 0; i < operationCounts.size(); i++)
    {
        SegmentInfo segmentInfo;
        OperationWriterPtr opWriter = CreateOperationWriter(
                mSchema, operationBlockSize, compressOperation);
        for (int32_t j = 0; j < operationCounts[i]; ++j)
        {
            stringstream docStr;
            docStr << "cmd=update_field,string1=pkStr,price="
                   << opCount << ",ts=" << segOpTsVec[i][j];
            maxTs = max(maxTs, segOpTsVec[i][j]);
            segmentInfo.timestamp = maxTs;
            ++opCount;
            NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docStr.str());
            opWriter->AddOperation(doc);
        }

        if (hasDumpingSegment && i == (operationCounts.size() - 1))
        {
            // last segment is dumping segment
            Version tmpVersion;
            tmpVersion = version;
            tmpVersion.SetVersionId(0);
            tmpVersion.Store(GET_PARTITION_DIRECTORY(), false);
            PartitionDataPtr partitionData =
                PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
            for (size_t idx = 0; idx < tmpVersion.GetSegmentCount(); idx++)
            {
                builtSegments.push_back(partitionData->GetSegmentData(tmpVersion[idx]));
            }
            InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();
            inMemSegment->SetOperationWriter(opWriter);
            *inMemSegment->GetSegmentWriter()->GetSegmentInfo() = segmentInfo;
            inMemSegment->SetStatus(InMemorySegment::WAITING_TO_DUMP);
            dumpingSegments.push_back(inMemSegment);
        }
        version.AddSegment(i);
        string segName = version.GetSegmentDirName(i);
        DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeDirectory(segName);
        assert(segDirectory);
        opWriter->Dump(segDirectory);
        segmentInfo.Store(segDirectory);
    }
    version.Store(GET_PARTITION_DIRECTORY(), false);
    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);
    if (builtSegments.empty())
    {
        for (size_t idx = 0; idx < version.GetSegmentCount(); idx++)
        {
            builtSegments.push_back(partitionData->GetSegmentData(version[idx]));
        }
    }
    InMemorySegmentPtr inMemSegment = partitionData->CreateNewSegment();

    OperationWriterPtr opWriter = CreateOperationWriter(
            mSchema, operationBlockSize, compressOperation);
    
    for (size_t i = 0; i < buildingOpCount; ++i)
    {
        stringstream docStr;
        docStr << "cmd=update_field,string1=pkStr,price="
               << opCount << ",ts=" << buildingOpTsVec[i];
        maxTs = max(maxTs, buildingOpTsVec[i]);
        ++opCount;
        NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docStr.str());
        opWriter->AddOperation(doc);
    }

    inMemSegment->SetOperationWriter(opWriter);
    buildingSegment = inMemSegment;

    FakePartitionDataPtr fakePartData(new FakePartitionData(partitionData));
    fakePartData->Init(builtSegments, dumpingSegments, buildingSegment);
    return fakePartData;
}


void OperationIteratorTest::GetTimestampVectorForTargetSegments(
        const vector<int32_t>& segOpCounts,
        size_t buildingOpCount, const string& opTimeStampStr,
        vector<vector<uint32_t> > &segOpTsVec,
        vector<uint32_t> &buildingOpTsVec)
{
    vector<uint32_t> opTsVec;
    StringUtil::fromString(opTimeStampStr, opTsVec, ",");
    uint32_t totalOpCount = 0;
    for (size_t i = 0; i < segOpCounts.size(); i++)
    {
        vector<uint32_t> tsVec;
        for (int32_t j = 0; j < segOpCounts[i]; j++)
        {
            uint32_t ts = totalOpCount;
            if (!opTsVec.empty())
            {
                assert(totalOpCount < (uint32_t)opTsVec.size());
                ts = opTsVec[totalOpCount];
            }
            tsVec.push_back(ts);
            totalOpCount++;
        }
        segOpTsVec.push_back(tsVec);
    }
    assert(buildingOpTsVec.empty());
    for (size_t i = 0; i < buildingOpCount; i++)
    {
        uint32_t ts = totalOpCount;
        if (!opTsVec.empty())
        {
            assert(totalOpCount < (uint32_t)opTsVec.size());
            ts = opTsVec[totalOpCount];
        }
        buildingOpTsVec.push_back(ts);
        totalOpCount++;
    }

    assert(opTsVec.size() == 0 || opTsVec.size() == totalOpCount);
}

IE_NAMESPACE_END(partition);

