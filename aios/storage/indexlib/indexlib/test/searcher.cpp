#include "indexlib/test/searcher.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/normal/summary/summary_reader.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/atomic_query.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/source_query.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::document;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, Searcher);

Searcher::Searcher() {}

Searcher::~Searcher() {}

bool Searcher::Init(const IndexPartitionReaderPtr& reader, config::IndexPartitionSchema* schema)
{
    mReader = reader;
    mSchema = schema;
    return true;
}

ResultPtr Searcher::Search(QueryPtr query, TableSearchCacheType searchCacheType, std::string& errorInfo)
{
    assert(searchCacheType == tsc_default);
    DeletionMapReaderPtr deletionMapReader = mReader->GetDeletionMapReader();

    std::stringstream ss;
    ResultPtr result(new test::Result);
    docid_t docid = INVALID_DOCID;
    do {
        docid = query->Seek(docid);
        auto atomicQuery = dynamic_pointer_cast<AtomicQuery>(query);
        if (docid != INVALID_DOCID) {
            bool isDeleted = (deletionMapReader && deletionMapReader->IsDeleted(docid));
            if (((query->GetQueryType() != qt_docid) && isDeleted)) {
                if (atomicQuery) {
                    ss << "[Deleted] pk=" << atomicQuery->GetWord() << " docId=" << docid << " is deleted";
                }
                continue;
            } else {
                if (atomicQuery) {
                    ss << "[Got] pk=" << atomicQuery->GetWord() << " docId=" << docid << " reader=" << mReader.get();
                }
            }
            errorInfo = ss.str();
            FillResult(query, docid, result, isDeleted);
        }
    } while (docid != INVALID_DOCID);

    return result;
}

ResultPtr Searcher::Search(QueryPtr query, TableSearchCacheType searchCacheType)
{
    std::string errorInfo;
    return Search(query, searchCacheType, errorInfo);
}

bool Searcher::GetSingleAttributeValue(const AttributeSchemaPtr& attrSchema, const string& attrName, docid_t docId,
                                       string& attrValue)
{
    assert(mReader);
    assert(attrSchema);

    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(attrName);
    PackAttributeConfig* packAttrConfig = attrConfig->GetPackAttributeConfig();
    Pool pool;
    if (packAttrConfig) // attribute belongs to pack attribute
    {
        string packName = packAttrConfig->GetPackName();
        PackAttributeReaderPtr packAttrReader = mReader->GetPackAttributeReader(packName);
        if (!packAttrReader) {
            return false;
        }
        return packAttrReader->Read(docId, attrName, attrValue, &pool);
    }

    AttributeReaderPtr attrReader = mReader->GetAttributeReader(attrName);
    if (!attrReader) {
        return false;
    }
    return attrReader->Read(docId, attrValue, &pool);
}

void Searcher::FillResult(QueryPtr query, docid_t docId, ResultPtr result, bool isDeleted)
{
    // fill attribute
    RawDocumentPtr rawDoc(new RawDocument);
    rawDoc->SetField(FIELD_DOCID, StringUtil::toString<docid_t>(docId));
    rawDoc->SetDocId(docId);
    rawDoc->SetField(FIELD_DELETION_MAP, isDeleted ? "1" : "0");
    FillPosition(query, rawDoc);

    SourceQueryPtr sourceQuery = dynamic_pointer_cast<SourceQuery>(query);
    if (sourceQuery) {
        assert(mSchema->GetSourceSchema());
        FillRawDocBySource(sourceQuery, docId, rawDoc);
        result->AddDoc(rawDoc);
        return;
    }
    FieldSchemaPtr fieldSchema = mSchema->GetFieldSchema();
    assert(fieldSchema);
    AttributeSchemaPtr attrSchema = mSchema->GetAttributeSchema();
    SummarySchemaPtr summarySchema = mSchema->GetSummarySchema();

    for (FieldSchema::Iterator it = fieldSchema->Begin(); it != fieldSchema->End(); ++it) {
        fieldid_t fieldId = (*it)->GetFieldId();
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            string fieldName = (*it)->GetFieldName();
            string fieldValue;
            GetSingleAttributeValue(attrSchema, fieldName, docId, fieldValue);

            if ((*it)->IsMultiValue()) {
                fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
            }
            rawDoc->SetField(fieldName, fieldValue);
        }
        if (summarySchema && summarySchema->IsInSummary(fieldId)) {
            string fieldName = (*it)->GetFieldName();
            SummaryReaderPtr summaryReader = mReader->GetSummaryReader();
            assert(summaryReader);
            SearchSummaryDocument summaryDoc(NULL, 4096);
            bool ret = summaryReader->GetDocument(docId, &summaryDoc);
            if (!ret) {
                rawDoc->SetField(fieldName, "__SUMMARY_FAILED__");
            }
            summaryfieldid_t summaryFieldId = summarySchema->GetSummaryFieldId(fieldId);
            const StringView* fieldValue = summaryDoc.GetFieldValue(summaryFieldId);
            if (!fieldValue) {
                continue;
            }
            string fieldValueStr = fieldValue->to_string();
            if ((*it)->IsMultiValue()) {
                fieldValueStr = DocumentParser::ParseMultiValueResult(fieldValueStr);
            }
            rawDoc->SetField(fieldName, fieldValueStr);
        }
    }
    result->AddDoc(rawDoc);
}

void Searcher::FillRawDocBySource(SourceQueryPtr query, docid_t docId, RawDocumentPtr rawDoc)
{
    groupid_t groupId = query->GetGroupId();
    SourceGroupConfigPtr groupConfig = mSchema->GetSourceSchema()->GetGroupConfig(groupId);
    SourceDocument sourceDoc(NULL);
    mReader->GetSourceReader()->GetDocument(docId, &sourceDoc);

    SourceDocument::SourceGroupFieldIter dataIter = sourceDoc.CreateGroupFieldIter(groupId);
    SourceDocument::SourceMetaIter metaIter = sourceDoc.CreateGroupMetaIter(groupId);
    while (dataIter.HasNext()) {
        assert(metaIter.HasNext());
        const StringView data = dataIter.Next();
        const StringView meta = metaIter.Next();
        rawDoc->SetField(string(meta.data(), meta.size()), string(data.data(), data.size()));
    }
}

void Searcher::FillPosition(QueryPtr query, RawDocumentPtr rawDoc)
{
    vector<pos_t> posVector;
    pos_t pos = 0;
    do {
        pos = query->SeekPosition(pos);
        if (pos != INVALID_POSITION) {
            posVector.push_back(pos);
        }
    } while (pos != INVALID_POSITION);
    if (posVector.size() != 0) {
        rawDoc->SetField(FIELD_POS, StringUtil::toString(posVector, FIELD_POS_SEP));
    }
}
}} // namespace indexlib::test
