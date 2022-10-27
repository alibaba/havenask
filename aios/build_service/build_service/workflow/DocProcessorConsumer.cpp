#include "build_service/workflow/DocProcessorConsumer.h"

using namespace std;
namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, DocProcessorConsumer);

DocProcessorConsumer::DocProcessorConsumer(processor::Processor *processor)
    : _processor(processor)
{
}

DocProcessorConsumer::~DocProcessorConsumer() {
}

FlowError DocProcessorConsumer::consume(const document::RawDocumentPtr &item) {
    _processor->processDoc(item);
    return FE_OK;
}

bool DocProcessorConsumer::getLocator(common::Locator &locator) const {
    locator = common::INVALID_LOCATOR;
    return true;
}

bool DocProcessorConsumer::stop(FlowError lastError) {
    _processor->stop();
    return true;
}

}
}
