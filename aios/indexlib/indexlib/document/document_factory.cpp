#include "indexlib/document/document_factory.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, DocumentFactory);

const string DocumentFactory::DOCUMENT_FACTORY_NAME = "DocumentFactory";

DocumentFactory::DocumentFactory() 
{
}

DocumentFactory::~DocumentFactory() 
{
}

RawDocument* DocumentFactory::CreateRawDocument(
        const config::IndexPartitionSchemaPtr& schema,
        const DocumentInitParamPtr& initParam)
{
    unique_ptr<RawDocument> rawDoc(CreateRawDocument(schema));
    if (!rawDoc)
    {
        ERROR_COLLECTOR_LOG(ERROR, "create raw document fail!");
        return nullptr;
    }
    if (!rawDoc->Init(initParam))
    {
        ERROR_COLLECTOR_LOG(ERROR, "init raw document fail!");
        return nullptr;
    }
    return rawDoc.release();
}

RawDocument* DocumentFactory::CreateMultiMessageRawDocument(
        const config::IndexPartitionSchemaPtr& schema,
        const DocumentInitParamPtr& initParam)
{
    unique_ptr<RawDocument> rawDoc(CreateMultiMessageRawDocument(schema));
    if (!rawDoc)
    {
        ERROR_COLLECTOR_LOG(ERROR, "create multi message raw document fail!");
        return nullptr;
    }
    if (!rawDoc->Init(initParam))
    {
        ERROR_COLLECTOR_LOG(ERROR, "init multi message raw document fail!");
        return nullptr;
    }
    return rawDoc.release();
}
    
DocumentParser* DocumentFactory::CreateDocumentParser(
        const IndexPartitionSchemaPtr& schema,
        const DocumentInitParamPtr& initParam)
{
    unique_ptr<DocumentParser> parser(CreateDocumentParser(schema));
    if (!parser)
    {
        ERROR_COLLECTOR_LOG(ERROR, "create document parser fail!");
        return nullptr;
    }

    if (!parser->Init(initParam))
    {
        ERROR_COLLECTOR_LOG(ERROR, "init DocumentParser fail!");
        return nullptr;
    }
    return parser.release();
}

IE_NAMESPACE_END(document);

