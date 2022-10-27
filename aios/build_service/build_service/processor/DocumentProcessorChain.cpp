#include "build_service/processor/DocumentProcessorChain.h"
#include "build_service/util/Monitor.h"

using namespace std;
using namespace build_service::document;

IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(util);

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, DocumentProcessorChain);

DocumentProcessorChain::DocumentProcessorChain()
    : _docSerializeVersion(INVALID_DOC_VERSION)
    , _tolerateFormatError(true)
    , _useRawDocAsDoc(false)
{
}

DocumentProcessorChain::DocumentProcessorChain(DocumentProcessorChain& other)
    : _docFactoryWrapper(other._docFactoryWrapper)
    , _docParser(other._docParser)
    , _docSerializeVersion(other._docSerializeVersion)
    , _tolerateFormatError(other._tolerateFormatError)
    , _useRawDocAsDoc(other._useRawDocAsDoc)
{
}

DocumentProcessorChain::~DocumentProcessorChain() {
}

ProcessedDocumentPtr DocumentProcessorChain::process(const RawDocumentPtr& rawDoc) {
    ExtendDocumentPtr extendDocument(new ExtendDocument);
    extendDocument->setRawDocument(rawDoc);
    IE_RAW_DOC_TRACE(rawDoc, "process begin");
    if (!processExtendDocument(extendDocument)) {
        return ProcessedDocumentPtr();
    }
    ProcessedDocumentPtr ret = handleExtendDocument(extendDocument);
    IE_RAW_DOC_TRACE(rawDoc, "process done");
    return ret;
}

ProcessedDocumentVecPtr DocumentProcessorChain::batchProcess(
        const RawDocumentVecPtr &batchRawDocs) {
    ExtendDocument::ExtendDocumentVec extDocVec;
    extDocVec.reserve(batchRawDocs->size());

    for (size_t i = 0; i < batchRawDocs->size(); i++) {
        ExtendDocumentPtr extendDocument(new ExtendDocument);
        extendDocument->setRawDocument((*batchRawDocs)[i]);
        extDocVec.push_back(extendDocument);
        IE_RAW_DOC_TRACE((*batchRawDocs)[i], "process begin");        
    }
    batchProcessExtendDocs(extDocVec);
    ProcessedDocumentVecPtr ret(new ProcessedDocumentVec);
    ret->reserve(batchRawDocs->size());
    for (size_t i = 0; i < extDocVec.size(); i++) {
        ret->push_back(handleExtendDocument(extDocVec[i]));
        const RawDocumentPtr& rawDoc = extDocVec[i]->getRawDocument();
        IE_RAW_DOC_TRACE(rawDoc, "process done");
    }
    return ret;
}


ProcessedDocumentPtr DocumentProcessorChain::handleExtendDocument(
        const ExtendDocumentPtr &extendDocument)
{
    if (extendDocument->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH))
    {
        return ProcessedDocumentPtr();
    }
    
    const ProcessedDocumentPtr &processedDocument = extendDocument->getProcessedDocument();
    DocOperateType opType = extendDocument->getRawDocument()->getDocOperateType();
    const RawDocumentPtr& rawDoc = extendDocument->getRawDocument();
    if (opType == DocOperateType::SKIP_DOC) {
        // skip doc
        processedDocument->setNeedSkip(true);
        IE_RAW_DOC_TRACE(rawDoc, "skip rawDoc.");
        return processedDocument;
    }
    
    IE_NAMESPACE(document)::DocumentPtr indexDocument;
    if (_useRawDocAsDoc) {
        indexDocument = rawDoc;
        IE_RAW_DOC_TRACE(rawDoc, "use rawDoc as indexDoc.");
    } else if (_docParser) {
        IE_INDEX_DOC_TRACE(indexDocument, "begin parse indexDoc");
        indexDocument = _docParser->Parse(extendDocument->getIndexExtendDoc());
        if (indexDocument) {
            indexDocument->SetTrace(extendDocument->getRawDocument()->NeedTrace());
        }
        IE_INDEX_DOC_TRACE(indexDocument, "parse indexDoc done");
    }

    if (!indexDocument)
    {
        BS_INTERVAL_LOG(300, ERROR, "create index document fail!");
        IE_RAW_DOC_TRACE(rawDoc, "parse rawDoc failed.");
        return ProcessedDocumentPtr();
    }

    if (_docSerializeVersion != INVALID_DOC_VERSION) {
        indexDocument->SetSerializedVersion(_docSerializeVersion);
    }
    handleBuildInProcessorWarnings(extendDocument, indexDocument);
    processedDocument->setDocument(indexDocument);
    processedDocument->setLocator(extendDocument->getRawDocument()->getLocator());
    return processedDocument;
}

void DocumentProcessorChain::handleBuildInProcessorWarnings(
    const ExtendDocumentPtr &extendDocument,
    const IE_NAMESPACE(document)::DocumentPtr& indexDocument)
{
    assert(extendDocument);
    const std::string& docIdentifier = extendDocument->getIdentifier();
    if (extendDocument->testWarningFlag(
            ProcessorWarningFlag::PWF_HASH_FIELD_EMPTY))
    {
        BS_LOG(ERROR, "hashField of Doc[pkField=%s] is missing", docIdentifier.c_str());
    }

    if (indexDocument->HasFormatError()) {
        IE_INDEX_DOC_TRACE(indexDocument, "doc has format error(check attributeConvertError)");
        if (!_tolerateFormatError) {
            indexDocument->SetDocOperateType(SKIP_DOC);
            BS_LOG(ERROR, "Doc[pkField=%s] has format error(check attributeConvertError), will be skipped",
                   docIdentifier.c_str());
        } else {
            BS_INTERVAL_LOG(50, WARN, "Doc[pkField=%s] has format error(check attributeConvertError),"
                            "will use default value in related fields", docIdentifier.c_str());
        }
    }
}

bool DocumentProcessorChain::init(
    const DocumentFactoryWrapperPtr& wrapper, const DocumentInitParamPtr& initParam,
    bool useRawDocAsDoc)
{
    _useRawDocAsDoc = useRawDocAsDoc;
    if (!wrapper)
    {
        return false;
    }
    _docFactoryWrapper = wrapper;
    _docParser.reset(_docFactoryWrapper->CreateDocumentParser(initParam));
    return _docParser.get() != nullptr;
}
    
}
}
