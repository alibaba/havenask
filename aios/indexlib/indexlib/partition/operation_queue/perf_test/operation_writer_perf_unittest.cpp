#include "indexlib/partition/operation_queue/perf_test/operation_writer_perf_unittest.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/document_creator.h"
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationWriterPerfTest);

OperationWriterPerfTest::OperationWriterPerfTest()
{
}

OperationWriterPerfTest::~OperationWriterPerfTest()
{
}

void OperationWriterPerfTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32;string3:string:true:true";
    string index = "index2:string:string2;"
        "pk:primarykey64:string1";

    string attribute = "string1;price;string3";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}    


void OperationWriterPerfTest::CaseTearDown()
{
}

void OperationWriterPerfTest::TestDumpPerf()
{
    string docString = 
        "cmd=update_field,string1=hello,price=5,string3=abcdefghigklmnopqrstuvwxyz0123456789,ts=6;";
    
    OperationWriterPtr opWriter(new OperationWriter());
    opWriter->Init(mSchema);

    NormalDocumentPtr doc = DocumentCreator::CreateDocument(mSchema, docString);
    int64_t startTs = 6;
    for (size_t i = 1; i <= 5000000; ++i)
    {
        doc->SetTimestamp(startTs++);
        opWriter->AddOperation(doc);
    }
    
    cout << "dump memory size : " << opWriter->GetDumpSize() << endl;
    cout << "writer memory use : " << opWriter->GetTotalMemoryUse() << endl;
    int64_t beginTime = TimeUtility::currentTime();
    opWriter->Dump(GET_SEGMENT_DIRECTORY());
    int64_t interval = TimeUtility::currentTime() - beginTime;
    cout << "***** time interval : " << interval / 1000 << "ms, " << endl;
}



IE_NAMESPACE_END(partition);

