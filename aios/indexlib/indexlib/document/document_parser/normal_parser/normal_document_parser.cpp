#include "indexlib/document/document_parser/normal_parser/normal_document_parser.h"
#include "indexlib/document/builtin_parser_init_param.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/accumulative_counter.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, NormalDocumentParser);

string NormalDocumentParser::ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME = "bs.processor.attributeConvertError";

NormalDocumentParser::NormalDocumentParser(const IndexPartitionSchemaPtr& schema)
    : DocumentParser(schema)
{
}

NormalDocumentParser::~NormalDocumentParser() 
{
}

bool NormalDocumentParser::DoInit()
{
    if (!InitFromDocumentParam())
    {
        return false;
    }
    mMainParser.reset(new SingleDocumentParser);
    if (!mMainParser->Init(mSchema, mAttributeConvertErrorCounter))
    {
        ERROR_COLLECTOR_LOG(ERROR, "init single document parser fail!");
        return false;
    }
    const IndexPartitionSchemaPtr& subSchema = mSchema->GetSubIndexPartitionSchema();
    if (subSchema)
    {
        mSubParser.reset(new SingleDocumentParser);
        if (!mSubParser->Init(
                mSchema->GetSubIndexPartitionSchema(), mAttributeConvertErrorCounter))
        {
            ERROR_COLLECTOR_LOG(ERROR, "init single document parser for sub schema fail!");
            return false;
        }
    }
    return true;
}

DocumentPtr NormalDocumentParser::Parse(const IndexlibExtendDocumentPtr& document)
{
    NormalDocumentPtr indexDoc = mMainParser->Parse(document);
    if (!indexDoc)
    {
        return DocumentPtr();
    }
    
    const RawDocumentPtr& rawDoc = document->getRawDocument();
    assert(rawDoc);
    int64_t timestamp = rawDoc->getDocTimestamp();
    DocOperateType opType = rawDoc->getDocOperateType();
    indexDoc->SetTimestamp(timestamp);
    indexDoc->SetSource(rawDoc->getDocSource());
    indexDoc->SetDocOperateType(opType);
    indexDoc->SetRegionId(document->getRegionId());
    indexDoc->SetSchemaVersionId(mSchema->GetSchemaVersionId());

    const IndexlibExtendDocument::IndexlibExtendDocumentVec& subDocuments =
        document->getAllSubDocuments();
    if (!mSubParser && !subDocuments.empty())
    {
        ERROR_COLLECTOR_LOG(ERROR, "not support parse sub documents!");
        return DocumentPtr();
    }
    for (size_t i = 0; i < subDocuments.size(); i++) {
        NormalDocumentPtr subIndexDoc = mSubParser->Parse(subDocuments[i]);
        if (!subIndexDoc)
        {
            continue;
        }
        subIndexDoc->SetTimestamp(timestamp);
        subIndexDoc->SetDocOperateType(opType);
        subIndexDoc->SetSchemaVersionId(mSchema->GetSchemaVersionId());
        indexDoc->AddSubDocument(subIndexDoc);
    }

    for (auto& rewriter : mDocRewriters)
    {
        if (rewriter)
        {
            rewriter->Rewrite(indexDoc);
        }
    }
    
    if (IsEmptyUpdate(indexDoc)) {
        indexDoc->SetDocOperateType(SKIP_DOC);
        IE_INCREASE_QPS(UselessUpdateQps);
    }

    // attention: make binary_version=7 document not used for modify operation and docTrace,
    // atomicly serialize to version6, to make online compitable
    // remove this code when binary version 6 is useless
    if (!mSchema->HasModifyOperations() && !(document->getRawDocument()->NeedTrace()) &&
        indexDoc->GetSerializedVersion() == 7)
    {
        indexDoc->SetSerializedVersion(6);
    }
    return indexDoc;
}

DocumentPtr NormalDocumentParser::Parse(const ConstString& serializedData)
{
    DataBuffer newDataBuffer(const_cast<char*>(serializedData.data()),
                             serializedData.length());
    bool hasObj;
    newDataBuffer.read(hasObj);
    if (hasObj)
    {
        uint32_t serializedVersion;
        newDataBuffer.read(serializedVersion);
        if (serializedVersion > DOCUMENT_BINARY_VERSION &&
            mSchema->GetSubIndexPartitionSchema() != NULL) {
            INDEXLIB_THROW(misc::BadParameterException,
                           "document serialize failed, "
                           "do not support serialize document with version %u", serializedVersion);
        }
    } else {
        INDEXLIB_THROW(misc::BadParameterException,
                       "document serialize failed");
    }
    DataBuffer dataBuffer(const_cast<char*>(serializedData.data()),
                          serializedData.length());
    NormalDocumentPtr document;
    dataBuffer.read(document);
    return document;
}

bool NormalDocumentParser::IsEmptyUpdate(const NormalDocumentPtr& document)
{
    if (UPDATE_FIELD != document->GetDocOperateType()) {
        return false;
    }
    if (0 != document->GetAttributeDocument()->GetNotEmptyFieldCount()) {
        return false;
    }
    const NormalDocument::DocumentVector &subDocuments = document->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++) {
        if (0 != subDocuments[i]->GetAttributeDocument()->GetNotEmptyFieldCount()) {
            return false;
        }
    }
    return true;
}

bool NormalDocumentParser::InitFromDocumentParam()
{
    if (!mInitParam)
    {
        IE_LOG(DEBUG, "empty init param!");
        return true;
    }

    BuiltInParserInitParamPtr param = DYNAMIC_POINTER_CAST(BuiltInParserInitParam, mInitParam);
    if (!param)
    {
        IE_LOG(WARN, "can not use BuiltInParserInitParam");
        return true;
    }

    const BuiltInParserInitResource& resource = param->GetResource();
    mDocRewriters = resource.docRewriters;
    IE_INIT_METRIC_GROUP(resource.metricProvider, UselessUpdateQps,
                         "debug/uselessUpdateQps_chain_" + mSchema->GetSchemaName(),
                         kmonitor::QPS, "count");

    if (resource.counterMap)
    {
        mAttributeConvertErrorCounter =
            resource.counterMap->GetAccCounter(ATTRIBUTE_CONVERT_ERROR_COUNTER_NAME);
        if (!mAttributeConvertErrorCounter)
        {
            IE_LOG(ERROR, "create attributeConvertErrorCounter failed");
        }
    }
    return true;
}

IE_NAMESPACE_END(document);

