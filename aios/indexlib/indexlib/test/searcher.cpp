#include "indexlib/test/searcher.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(testlib);
IE_NAMESPACE_USE(partition);
IE_NAMESPACE_USE(document);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, Searcher);

Searcher::Searcher() 
{
}

Searcher::~Searcher() 
{
}

bool Searcher::Init(const IndexPartitionReaderPtr& reader,
                    config::IndexPartitionSchema* schema)
{ 
    mReader = reader; 
    mSchema = schema;
    return true; 
}

ResultPtr Searcher::Search(QueryPtr query, TableSearchCacheType searchCacheType)
{
    assert(searchCacheType == tsc_default);
    DeletionMapReaderPtr deletionMapReader = 
        mReader->GetDeletionMapReader();

    ResultPtr result(new Result);
    docid_t docid = INVALID_DOCID;
    do
    {
        docid = query->Seek(docid);
        if (docid != INVALID_DOCID)
        {
            bool isDeleted = (deletionMapReader && deletionMapReader->IsDeleted(docid));
            if (((query->GetQueryType() != qt_docid) && isDeleted))
            {
                continue;
            }
            FillResult(query, docid, result, isDeleted);
        }
    }
    while (docid != INVALID_DOCID);

    return result; 
}

bool Searcher::GetSingleAttributeValue(
    const AttributeSchemaPtr& attrSchema, const string& attrName,
    docid_t docId, string& attrValue)
{
    assert(mReader);
    assert(attrSchema);

    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrName);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    Pool pool;
    if (packAttrConfig) // attribute belongs to pack attribute
    {
        string packName = packAttrConfig->GetAttrName();
        PackAttributeReaderPtr packAttrReader = mReader->GetPackAttributeReader(packName);
        if (!packAttrReader)
        {
            return false;
        }
        return packAttrReader->Read(docId, attrName, attrValue, &pool);
    }
    
    AttributeReaderPtr attrReader = mReader->GetAttributeReader(attrName);
    if (!attrReader)
    {
        return false;
    }
    return attrReader->Read(docId, attrValue, &pool);
}

void Searcher::FillResult(QueryPtr query, docid_t docId, ResultPtr result, bool isDeleted)
{
    //fill attribute
    RawDocumentPtr rawDoc(new RawDocument);
    rawDoc->SetField(FIELD_DOCID, StringUtil::toString<docid_t>(docId));
    rawDoc->SetDocId(docId);
    rawDoc->SetField(FIELD_DELETION_MAP, isDeleted?"1":"0");
    FillPosition(query, rawDoc);
    
    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    assert(fieldSchema);
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) 
        {
            string fieldName = (*it)->GetFieldName();
            string fieldValue;
            GetSingleAttributeValue(attrSchema, fieldName, docId, fieldValue);

            if ((*it)->IsMultiValue())
            {
                fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
            }
            rawDoc->SetField(fieldName, fieldValue);
        }
        if (summarySchema && summarySchema->IsInSummary(fieldId))
        {
            string fieldName = (*it)->GetFieldName();
            SummaryReaderPtr summaryReader = mReader->GetSummaryReader();
            assert(summaryReader);
            SearchSummaryDocument summaryDoc(NULL, 4096);
            bool ret = summaryReader->GetDocument(docId, &summaryDoc);
            if (!ret)
            {
                rawDoc->SetField(fieldName, "__SUMMARY_FAILED__");
            }
            summaryfieldid_t summaryFieldId = 
                summarySchema->GetSummaryFieldId(fieldId);
            const ConstString* fieldValue = 
                summaryDoc.GetFieldValue(summaryFieldId);
            if (!fieldValue) 
            {
                continue;
            }
            string fieldValueStr = fieldValue->toString();
            if ((*it)->IsMultiValue())
            {
                fieldValueStr = DocumentParser::ParseMultiValueResult(
                        fieldValueStr);
            }
            rawDoc->SetField(fieldName, fieldValueStr);
        }
    }
    result->AddDoc(rawDoc);
}

void Searcher::FillPosition(QueryPtr query, RawDocumentPtr rawDoc)
{
    vector<pos_t> posVector;
    pos_t pos = 0;
    do
    {
        pos = query->SeekPosition(pos);
        if (pos != INVALID_POSITION)
        {
            posVector.push_back(pos);
        }
    } 
    while (pos != INVALID_POSITION);
    if (posVector.size() != 0)
    {
        rawDoc->SetField(FIELD_POS, StringUtil::toString(posVector, FIELD_POS_SEP));
    }
}

IE_NAMESPACE_END(test);

