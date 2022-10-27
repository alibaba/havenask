#include "indexlib/testlib/document_creator.h"
#include "indexlib/testlib/document_parser.h"
#include "indexlib/testlib/ExtendDocField2IndexDocField.h"
#include "indexlib/document/document_parser/kv_parser/kv_key_extractor.h"
#include "indexlib/document/document_parser/kv_parser/kkv_keys_extractor.h"
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/testlib/sub_document_extractor.h"
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/impl/kkv_index_config_impl.h"
#include "indexlib/document/index_document/kv_document/kv_index_document.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(testlib);
IE_LOG_SETUP(testlib, DocumentCreator);

NormalDocumentPtr DocumentCreator::CreateDocument(
    const IndexPartitionSchemaPtr& schema, const string& docString,
    bool needRewriteSectionAttribute)
{
    RawDocumentPtr rawDoc = DocumentParser::Parse(docString);
    assert(rawDoc);

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    vector<RawDocumentPtr> subDocs;
    if (subSchema)
    {
        SubDocumentExtractor extractor(schema);
        extractor.extractSubDocuments(rawDoc, subDocs);
    }

    NormalDocumentPtr doc = CreateDocument(schema, rawDoc);
    if (subSchema)
    {
        for (size_t i = 0; i < subDocs.size(); ++i)
        {
            NormalDocumentPtr subDoc = CreateDocument(subSchema, subDocs[i]);
            doc->AddSubDocument(subDoc);
        }
    }

    if (needRewriteSectionAttribute)
    {
	SectionAttributeRewriter rewriter;
	if (rewriter.Init(schema))
	{
	    rewriter.Rewrite(doc);
	}
    }

    return doc;
}

void DocumentCreator::SetPrimaryKey(config::IndexPartitionSchemaPtr schema,
                                    RawDocumentPtr rawDoc,
                                    DocumentPtr document,
                                    bool isLegacyKvIndexDocument)
{
    assert(document);
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema(doc->GetRegionId());
    if (!indexSchema->HasPrimaryKeyIndex())
    {
        return;
    }
    // inverted index doc
    if (schema->GetTableType() == tt_index)
    {
        string pk;
        string pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
        pk = rawDoc->GetField(pkFieldName);
        IndexDocumentPtr indexDoc = doc->GetIndexDocument();
        assert(indexDoc);
        indexDoc->SetPrimaryKey(pk);
        return ;
    }
}

void DocumentCreator::SetDocumentValue(config::IndexPartitionSchemaPtr schema,
                                       RawDocumentPtr rawDoc,
                                       DocumentPtr document)
{
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    regionid_t regionId = doc->GetRegionId();

    ExtendDocField2IndexDocField convert(schema, regionId);
    FieldSchemaPtr fieldSchema = schema->GetFieldSchema(regionId);
    assert(fieldSchema);
    AttributeSchemaPtr attrSchema = schema->GetAttributeSchema(regionId);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema(regionId);
    SummarySchemaPtr summarySchema = schema->GetSummarySchema(regionId);

    SummaryDocumentPtr summaryDoc(new SummaryDocument);

    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        fieldid_t fieldId = (*it)->GetFieldId();

        if (indexSchema && indexSchema->IsInIndex(fieldId))
        {
            convert.convertIndexField(doc, *it, rawDoc);
        }

        if (attrSchema && attrSchema->IsInAttribute(fieldId))
        {
            convert.convertAttributeField(doc, *it, rawDoc);
        }
        else if (summarySchema && summarySchema->IsInSummary(fieldId))
        {
            convert.convertSummaryField(summaryDoc, *it, rawDoc);
        }
    }

    IE_NAMESPACE(document)::SerializedSummaryDocumentPtr serSummaryDoc;
    if (schema->NeedStoreSummary())
    {
        IE_NAMESPACE(document)::SummaryFormatter summaryFormatter(summarySchema);
        summaryFormatter.SerializeSummaryDoc(summaryDoc, serSummaryDoc);
    }
    doc->SetSummaryDocument(serSummaryDoc);

    string modifyFields = rawDoc->GetField(RESERVED_MODIFY_FIELDS);
    convert.convertModifyFields(doc, modifyFields);
}

vector<NormalDocumentPtr> DocumentCreator::CreateDocuments(
        const config::IndexPartitionSchemaPtr& schema,
        const string& docString,
	bool needRewriteSectionAttribute)
{
    vector<string> docStrings = StringUtil::split(docString,
            DocumentParser::DP_CMD_SEPARATOR);
    vector<NormalDocumentPtr> docs;
    for (size_t i = 0; i < docStrings.size(); ++i)
    {
        NormalDocumentPtr doc = CreateDocument(
                schema, docStrings[i], needRewriteSectionAttribute);
        assert(doc);
        docs.push_back(doc);
    }
    return docs;
}

NormalDocumentPtr DocumentCreator::CreateDocument(
        const config::IndexPartitionSchemaPtr& schema,
        RawDocumentPtr rawDoc)
{
    NormalDocumentPtr doc(new NormalDocument);
    doc->SetDocOperateType(rawDoc->GetDocOperateType());
    doc->SetTimestamp(rawDoc->GetTimestamp());
    doc->SetUserTimestamp(autil::StringUtil::strToInt64WithDefault(rawDoc->GetField(HA_RESERVED_TIMESTAMP).c_str(), INVALID_TIMESTAMP));
    doc->SetLocator(rawDoc->GetLocator());
    IndexDocumentPtr indexDoc(new IndexDocument(doc->GetPool()));
    doc->SetIndexDocument(indexDoc);
    AttributeDocumentPtr attrDoc;
    if (schema && schema->GetAttributeSchema())
    {
        attrDoc.reset(new AttributeDocument);
    }
    doc->SetAttributeDocument(attrDoc);
    doc->SetSchemaVersionId(schema->GetSchemaVersionId());

    SetPrimaryKey(schema, rawDoc, doc, false);

    DocOperateType op = doc->GetDocOperateType();
    if (op == ADD_DOC || op == UPDATE_FIELD)
    {
        SetDocumentValue(schema, rawDoc, doc);
    }
    return doc;
}

regionid_t DocumentCreator::ExtractRegionIdFromRawDocument(
        const IndexPartitionSchemaPtr& schema,
        const RawDocumentPtr& rawDocument,
        const string& defaultRegionName)
{
    if (schema->GetRegionCount() == 1)
    {
        return DEFAULT_REGIONID;
    }

    const string& field = rawDocument->GetField(RESERVED_REGION_NAME);

    if (field.empty() && !defaultRegionName.empty())
    {
        return schema->GetRegionId(defaultRegionName);
    }
    return schema->GetRegionId(field);
}

IE_NAMESPACE_END(testlib);
