#include "build_service/workflow/DocProcessorProducer.h"
#include "build_service/document/ProcessedDocument.h"

using namespace std;
using namespace build_service::document;
namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, DocProcessorProducer);

DocProcessorProducer::DocProcessorProducer(processor::Processor *processor)
    : _processor(processor)
{
}

DocProcessorProducer::~DocProcessorProducer() {
}

FlowError DocProcessorProducer::produce(
        document::ProcessedDocumentVecPtr &processedDocVec)
{
    processedDocVec = _processor->getProcessedDoc();
    return processedDocVec ? FE_OK : FE_EOF;
}

bool DocProcessorProducer::seek(const common::Locator &locator) {
    return true;
}

bool DocProcessorProducer::stop() {
    return true;
}

}
}
