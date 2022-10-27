#include "build_service/processor/SubDocumentExtractorProcessor.h"
#include "build_service/document/RawDocument.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::document;
IE_NAMESPACE_USE(config);

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, SubDocumentExtractorProcessor);

const string SubDocumentExtractorProcessor::PROCESSOR_NAME = "SubDocumentExtractorProcessor";

SubDocumentExtractorProcessor::SubDocumentExtractorProcessor()
    : DocumentProcessor(ADD_DOC | UPDATE_FIELD | DELETE_DOC | DELETE_SUB_DOC)
{
}

SubDocumentExtractorProcessor::~SubDocumentExtractorProcessor() {
}

bool SubDocumentExtractorProcessor::process(const ExtendDocumentPtr &document) {
    assert(_subDocumentExtractor);
    assert(document);

    const RawDocumentPtr& origRawDocument = document->getRawDocument();
    assert(origRawDocument);

    vector<RawDocumentPtr> subRawDocuments;

    _subDocumentExtractor->extractSubDocuments(origRawDocument, subRawDocuments);

    DocOperateType opType = document->getRawDocument()->getDocOperateType();

    for (size_t i = 0; i < subRawDocuments.size(); ++i) {
        subRawDocuments[i]->setDocOperateType(opType);
        ExtendDocumentPtr subDoc(new ExtendDocument());
        subDoc->setRawDocument(subRawDocuments[i]);
        document->addSubDocument(subDoc);
    }

    return true;
}

void SubDocumentExtractorProcessor::batchProcess(const vector<ExtendDocumentPtr> &docs) {
    for (size_t i = 0; i < docs.size(); i++) {
        if (!process(docs[i])) {
            docs[i]->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

void SubDocumentExtractorProcessor::destroy() {
    delete this;
}

DocumentProcessor* SubDocumentExtractorProcessor::clone()
{
    return new SubDocumentExtractorProcessor(*this);
}

bool SubDocumentExtractorProcessor::init(const KeyValueMap &kvMap,
        const IndexPartitionSchemaPtr &schemaPtr,
        ResourceReader *resourceReader)
{
    assert(schemaPtr);
    _schema = schemaPtr;
    _subDocumentExtractor.reset(new SubDocumentExtractor(_schema));
    return true;
}

}
}
