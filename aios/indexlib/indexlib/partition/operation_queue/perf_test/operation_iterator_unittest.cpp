#include "indexlib/partition/operation_queue/perf_test/operation_iterator_unittest.h"
#include "indexlib/test/partition_data_maker.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/test/document_creator.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(partition);

OperationIteratorPerfTest::OperationIteratorPerfTest()
{
}

OperationIteratorPerfTest::~OperationIteratorPerfTest()
{
}

void OperationIteratorPerfTest::CaseSetUp()
{
    string field = "string1:string;price:uint32";
    string index = "pk:primarykey64:string1";
    string attribute = "price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void OperationIteratorPerfTest::CaseTearDown()
{
}

void OperationIteratorPerfTest::TestLoadPerf()
{
    string docString = 
        "cmd=update_field,string1=hello,price=5,string3=abcdefghigklmnopqrstuvwxyz0123456789,ts=6;";
    
    vector<NormalDocumentPtr> docs = 
        DocumentCreator::CreateDocuments(mSchema, docString);

    Version version;
    size_t segCount = 10;
    int32_t opCountPerSeg = 5000000;
    OperationCursor opCursor(-1, -1);
    
    for (size_t i = 0; i < segCount; i++)
    {
        SegmentInfo segmentInfo;
        OperationWriter opWriter;
        opWriter.Init(mSchema);
        for (int32_t j = 0; j < opCountPerSeg; ++j)
        {
            docs[0]->SetTimestamp(i * opCountPerSeg + j);
            opWriter.AddOperation(docs[0]);
        }
        segmentInfo.timestamp = (i + 1) * opCountPerSeg - 1;
        string segName = "segment_" + StringUtil::toString(i) + "_level_0";
        DirectoryPtr segDirectory = GET_PARTITION_DIRECTORY()->MakeInMemDirectory(segName);
        assert(segDirectory);
        opWriter.Dump(segDirectory);
        segmentInfo.Store(segDirectory);
        version.AddSegment(i);
    }
    version.Store(GET_PARTITION_DIRECTORY());
    IndexPartitionOptions options;
    options.SetIsOnline(false);
    options.GetOnlineConfig().enableCompressOperationBlock = false;
    PartitionDataPtr partitionData =
        PartitionDataMaker::CreatePartitionData(GET_FILE_SYSTEM(), options, mSchema);

    OperationIterator opIter(partitionData, mSchema);
    opIter.Init(-1, opCursor);

    int64_t beginTime = TimeUtility::currentTime();
    int64_t lastTimestamp = -1;
    int count = 0;
    OperationBase* operation = NULL;
    while (opIter.HasNext())
    {
        operation = opIter.Next();
        lastTimestamp = operation->GetTimestamp();
        ++count;
    }
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "count is : " << count << endl;
    cout << "timestamp is : " << lastTimestamp<< endl;
    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;    
}

IE_NAMESPACE_END(operation_queue);

