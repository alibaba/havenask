#include "indexlib/document/document_parser/kv_parser/kv_document_parser.h"
#include "indexlib/document/document_parser/kv_parser/multi_region_kv_key_extractor.h"
#include "indexlib/document/document_parser/normal_parser/multi_region_extend_doc_fields_convertor.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/document_rewriter/multi_region_pack_attribute_appender.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/misc/doc_tracer.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, KvDocumentParser);

KvDocumentParser::KvDocumentParser(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
{
}

KvDocumentParser::~KvDocumentParser() 
{
}

bool KvDocumentParser::DoInit()
{
    mFieldConvertPtr.reset(new MultiRegionExtendDocFieldsConvertor(mSchema));
    MultiRegionPackAttributeAppenderPtr packAttrAppender(
            new MultiRegionPackAttributeAppender());
    if (packAttrAppender->Init(mSchema))
    {
        mPackAttrAppender = packAttrAppender;
    }
    return InitKeyExtractor();
}

DocumentPtr KvDocumentParser::Parse(const IndexlibExtendDocumentPtr& document)
{
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    if (!rawDoc)
    {        
        IE_LOG(ERROR, "empty raw document!");
        return DocumentPtr();
    }
    DocOperateType opType = rawDoc->getDocOperateType();
    if (opType == SKIP_DOC || opType == UNKNOWN_OP)
    {
        return CreateDocument(document);
    }

    if (opType == DELETE_SUB_DOC || opType == UPDATE_FIELD)
    {
        ERROR_COLLECTOR_LOG(ERROR, "unsupported document cmd type [%d]", opType);
        IE_RAW_DOC_FORMAT_TRACE(rawDoc, "unsupported document cmd type [%d]", opType);
        return DocumentPtr();
    }

    regionid_t regionId = document->getRegionId();
    const FieldSchemaPtr &fieldSchema = mSchema->GetFieldSchema(regionId);
    const IndexSchemaPtr &indexSchema = mSchema->GetIndexSchema(regionId);
    const AttributeSchemaPtr &attrSchema = mSchema->GetAttributeSchema(regionId);
    assert(indexSchema);
    SetPrimaryKeyField(document, indexSchema, regionId, opType);
    if (opType == DELETE_DOC)
    {
        return CreateDocument(document);
    }
    
    for (FieldSchema::Iterator it = fieldSchema->Begin();
         it != fieldSchema->End(); ++it)
    {
        assert((*it)->IsNormal());
        fieldid_t fieldId = (*it)->GetFieldId();
        if (attrSchema && attrSchema->IsInAttribute(fieldId)) {
            mFieldConvertPtr->convertAttributeField(document, *it);
        }
    }

    const ClassifiedDocumentPtr &classifiedDocument = document->getClassifiedDocument();
    if (mPackAttrAppender) {
        mPackAttrAppender->AppendPackAttribute(
                classifiedDocument->getAttributeDoc(),
                classifiedDocument->getPool(), regionId);
    }
    return CreateDocument(document);
}

DocumentPtr KvDocumentParser::Parse(const ConstString& serializedData)
{
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()),
                          serializedData.length());
    NormalDocumentPtr document;
    dataBuffer.read(document);
    return document;
}

void KvDocumentParser::SetPrimaryKeyField(
        const IndexlibExtendDocumentPtr &document,
        const IndexSchemaPtr &indexSchema, regionid_t regionId,
        DocOperateType opType)
{
    const string& pkFieldName = indexSchema->GetPrimaryKeyIndexFieldName();
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    if (pkFieldName.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "key field is empty!");
        IE_RAW_DOC_TRACE(rawDoc, "parse error: key field is empty");    
        return;
    }

    const ClassifiedDocumentPtr& classifiedDoc = document->getClassifiedDocument();
    string pkValue = rawDoc->getField(pkFieldName);
    document->setIdentifier(pkValue);

    uint64_t keyHash = 0;
    mKvKeyExtractor->HashKey(pkValue, keyHash, regionId);
    classifiedDoc->createKVIndexDocument(keyHash, 0, false);
}

bool KvDocumentParser::InitKeyExtractor()
{
    if (mSchema->GetTableType() != tt_kv)
    {
        ERROR_COLLECTOR_LOG(ERROR, "init KvDocumentParser fail, "
                            "unsupport tableType [%s]!",
                            IndexPartitionSchema::TableType2Str(
                                    mSchema->GetTableType()).c_str());
        return false;
    }
    mKvKeyExtractor.reset(new MultiRegionKVKeyExtractor(mSchema));
    return true;
}

DocumentPtr KvDocumentParser::CreateDocument(const IndexlibExtendDocumentPtr &document)
{
    const RawDocumentPtr &rawDoc = document->getRawDocument();
    const ClassifiedDocumentPtr &classifiedDoc = document->getClassifiedDocument();
    const KVIndexDocumentPtr &kvIndexDocument = classifiedDoc->getKVIndexDocument();
    if (!kvIndexDocument) {
        ERROR_COLLECTOR_LOG(ERROR, "getKVIndexDocument failed");
        IE_RAW_DOC_TRACE(rawDoc, "parse error: getKVIndexDocument failed");
        return DocumentPtr();
    }
    NormalDocumentPtr indexDoc(new NormalDocument(classifiedDoc->getPoolPtr()));
    indexDoc->SetKVIndexDocument(kvIndexDocument);
    indexDoc->SetAttributeDocument(classifiedDoc->getAttributeDoc());
    classifiedDoc->clear();

    int64_t timestamp = rawDoc->getDocTimestamp();
    DocOperateType opType = rawDoc->getDocOperateType();
    indexDoc->SetTimestamp(timestamp);
    indexDoc->SetSource(rawDoc->getDocSource());
    indexDoc->SetDocOperateType(opType);
    indexDoc->SetRegionId(document->getRegionId());
    
    // attention: make binary_version=7 document not used for docTrace,
    // atomicly serialize to version6, to make online compitable
    // remove this code when binary version 6 is useless
    if (!document->getRawDocument()->NeedTrace() && indexDoc->GetSerializedVersion() == 7)
    {
        indexDoc->SetSerializedVersion(6);
    }
    return indexDoc;
}

IE_NAMESPACE_END(document);

