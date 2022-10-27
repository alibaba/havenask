#include "build_service/processor/MainSubDocProcessorChain.h"

using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, MainSubDocProcessorChain);

MainSubDocProcessorChain::MainSubDocProcessorChain(DocumentProcessorChain *mainDocProcessor,
        DocumentProcessorChain *subDocProcessor)
    : _mainDocProcessor(mainDocProcessor)
    , _subDocProcessor(subDocProcessor)
{
}

MainSubDocProcessorChain::MainSubDocProcessorChain(MainSubDocProcessorChain &other)
    : DocumentProcessorChain(other)
{
    _mainDocProcessor = other._mainDocProcessor->clone();
    _subDocProcessor = other._subDocProcessor->clone();
}

MainSubDocProcessorChain::~MainSubDocProcessorChain() {
    delete _mainDocProcessor;
    delete _subDocProcessor;
}

bool MainSubDocProcessorChain::processExtendDocument(const ExtendDocumentPtr &extendDocument) {
    if (!_mainDocProcessor->processExtendDocument(extendDocument)) {
        return false;
    }
    const IndexlibExtendDocumentPtr& indexExtDoc = extendDocument->getIndexExtendDoc();
    for (size_t i = 0; i < extendDocument->getSubDocumentsCount(); ) {
        if (!_subDocProcessor->processExtendDocument(extendDocument->getSubDocument(i))) {
            extendDocument->removeSubDocument(i);
            continue;
        }

        indexExtDoc->addSubDocument(extendDocument->getSubDocument(i)->getIndexExtendDoc());
        ++i;
    }
    if (indexExtDoc->getSubDocumentsCount() != extendDocument->getSubDocumentsCount()) {
        BS_LOG(ERROR, "subDocCount not match [%lu,%lu]",
               indexExtDoc->getSubDocumentsCount(),
               extendDocument->getSubDocumentsCount());
        return false;
    }
    return true;
}

void MainSubDocProcessorChain::batchProcessExtendDocs(const vector<ExtendDocumentPtr>& extDocVec) {
    _mainDocProcessor->batchProcessExtendDocs(extDocVec);
    for (auto& extendDocument : extDocVec) {
        if (extendDocument->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
            continue;
        }

        _subDocProcessor->batchProcessExtendDocs(extendDocument->getAllSubDocuments());
        const IndexlibExtendDocumentPtr& indexExtDoc = extendDocument->getIndexExtendDoc();
        for (size_t i = 0; i < extendDocument->getSubDocumentsCount(); ) {
            if (extendDocument->getSubDocument(i)->testWarningFlag(
                            ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH))
            {
                extendDocument->removeSubDocument(i);
                continue;
            }
            indexExtDoc->addSubDocument(extendDocument->getSubDocument(i)->getIndexExtendDoc());
            ++i;
        }
        if (indexExtDoc->getSubDocumentsCount() != extendDocument->getSubDocumentsCount()) {
            BS_LOG(ERROR, "subDocCount not match [%lu,%lu]",
                   indexExtDoc->getSubDocumentsCount(),
                   extendDocument->getSubDocumentsCount());
            extendDocument->setWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH);
        }
    }
}

DocumentProcessorChain *MainSubDocProcessorChain::clone() {
    return new MainSubDocProcessorChain(*this);
}


}
}
