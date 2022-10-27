#include "indexlib/index/normal/summary/test/summary_maker.h"
#include "indexlib/index/normal/summary/summary_writer_impl.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/index/test/index_test_util.h"

using namespace std;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SummaryMaker);

SummaryMaker::SummaryMaker() 
{
}

SummaryMaker::~SummaryMaker() 
{
}

SummaryDocumentPtr SummaryMaker::MakeOneDocument(
        docid_t docId, const IndexPartitionSchemaPtr& indexPartitionSchema,
        autil::mem_pool::Pool *pool)
{
    SummaryDocumentPtr document(new SummaryDocument());
    document->SetDocId(docId);
    
    const FieldSchemaPtr fieldSchema = indexPartitionSchema->GetFieldSchema();
    for (uint32_t i = 0; i < fieldSchema->GetFieldCount(); ++i)
    {
        FieldConfigPtr fieldConfig = fieldSchema->GetFieldConfig((fieldid_t)i);
        stringstream ss;
        ss << "content_" << docId  << "_" << i;
        
        fieldid_t fieldId = fieldConfig->GetFieldId();
        if (indexPartitionSchema->GetSummarySchema()->IsInSummary((fieldid_t)i))
        {
            document->SetField(fieldId, autil::ConstString(ss.str(), pool));
        }
    }
 
    return document;
}

SummaryWriterPtr SummaryMaker::BuildOneSegmentWithoutDump(uint32_t docCount, 
        const IndexPartitionSchemaPtr& schema, autil::mem_pool::Pool *pool,
        DocumentArray& answerDocArray)
{
    SummaryWriterPtr writer(new SummaryWriterImpl());
    writer->Init(schema->GetSummarySchema(), NULL);
    for (size_t j = 0; j < docCount; ++j)
    {
        SummaryDocumentPtr doc(MakeOneDocument(j, schema, pool));
        SerializedSummaryDocumentPtr serDoc;
        SummaryFormatter formatter(schema->GetSummarySchema());
        formatter.SerializeSummaryDoc(doc, serDoc);
        writer->AddDocument(serDoc);
        doc->SetDocId(answerDocArray.size());
        answerDocArray.push_back(doc);
    }

    return writer;
}

void SummaryMaker::BuildOneSegment(const file_system::DirectoryPtr& segDirectory, 
                                   segmentid_t segId, 
                                   config::IndexPartitionSchemaPtr& schema,
                                   uint32_t docCount, autil::mem_pool::Pool *pool,
                                   DocumentArray& answerDocArray)
{
    SummaryWriterPtr writer = BuildOneSegmentWithoutDump(
            docCount, schema, pool, answerDocArray);

    file_system::DirectoryPtr attrDirectory = 
        segDirectory->MakeDirectory(SUMMARY_DIR_NAME);
    writer->Dump(attrDirectory);

    SegmentInfo segInfo;
    segInfo.docCount = docCount;
    segInfo.Store(segDirectory);
}


IE_NAMESPACE_END(index);

