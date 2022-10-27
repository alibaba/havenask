#include "build_service/processor/SingleDocProcessorChain.h"
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/document/ProcessedDocument.h"

using namespace std;
using namespace build_service::document;

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, SingleDocProcessorChain);

SingleDocProcessorChain::SingleDocProcessorChain(
        const plugin::PlugInManagerPtr &pluginManagerPtr)
    : _pluginManagerPtr(pluginManagerPtr)
{
}

SingleDocProcessorChain::SingleDocProcessorChain(
        SingleDocProcessorChain &other)
    : DocumentProcessorChain(other)
    , _pluginManagerPtr(other._pluginManagerPtr)
{
    _documentProcessors.resize(other._documentProcessors.size());
    for (size_t i = 0; i < _documentProcessors.size(); ++i) {
        _documentProcessors[i] = other._documentProcessors[i]->allocate();
    }
}

SingleDocProcessorChain::~SingleDocProcessorChain() {
    for (std::vector<DocumentProcessor*>::iterator it = _documentProcessors.begin();
         it != _documentProcessors.end(); ++it) {
        (*it)->deallocate();
    }
}

bool SingleDocProcessorChain::processExtendDocument(
        const document::ExtendDocumentPtr &extendDocument)
{
    for (vector<DocumentProcessor*>::iterator it = _documentProcessors.begin();
         it != _documentProcessors.end(); ++it)
    {
        DocOperateType docOperateType = extendDocument->getRawDocument()->getDocOperateType();
        if (!(*it)->needProcess(docOperateType)) {
            continue;
        }
        if (!(*it)->process(extendDocument)) {
            return false;
        }
    }
    return true;
}

DocumentProcessorChain *SingleDocProcessorChain::clone() {
    return new SingleDocProcessorChain(*this);
}

void SingleDocProcessorChain::addDocumentProcessor(DocumentProcessor *processor) {
    _documentProcessors.push_back(processor);
}

void SingleDocProcessorChain::batchProcessExtendDocs(
        const vector<ExtendDocumentPtr>& extDocVec)
{
    vector<ExtendDocumentPtr> needProcessDocs;
    needProcessDocs.reserve(extDocVec.size());
    for (vector<DocumentProcessor*>::iterator it = _documentProcessors.begin();
         it != _documentProcessors.end(); ++it)
    {
        needProcessDocs.clear();
        for (const auto& extDoc: extDocVec)
        {
            if (extDoc->testWarningFlag(ProcessorWarningFlag::PWF_PROCESS_FAIL_IN_BATCH)) {
                continue;
            }

            DocOperateType docOperateType = extDoc->getRawDocument()->getDocOperateType();
            if ((*it)->needProcess(docOperateType)) {
                needProcessDocs.push_back(extDoc);
            }
        }
        if (!needProcessDocs.empty()) {
            (*it)->batchProcess(needProcessDocs);
        }
    }
}


}
}
