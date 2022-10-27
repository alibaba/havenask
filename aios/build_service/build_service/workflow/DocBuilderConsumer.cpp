#include "build_service/workflow/DocBuilderConsumer.h"
#include "build_service/document/ProcessedDocument.h"
using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, DocBuilderConsumer);

DocBuilderConsumer::DocBuilderConsumer(builder::Builder *builder)
    : _builder(builder)
    , _endTimestamp(INVALID_TIMESTAMP)
{
}

DocBuilderConsumer::~DocBuilderConsumer() {
}

FlowError DocBuilderConsumer::consume(const ProcessedDocumentVecPtr &item) {
    assert(item->size() == 1);
    const ProcessedDocumentPtr &processedDoc = (*item)[0];
    const DocumentPtr &indexDoc = processedDoc->getDocument();
    if (!indexDoc) {
        return FE_OK;
    }
    common::Locator locator = processedDoc->getLocator();
    BS_LOG(DEBUG, "build locator is: %s", locator.toDebugString().c_str());
    indexDoc->SetLocator(locator.toString());

    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }
    if (_builder->build(indexDoc)) {
        return FE_OK;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }

    return FE_EXCEPTION;
}

bool DocBuilderConsumer::getLocator(common::Locator &locator) const {
    if (!_builder->getLastLocator(locator)) {
        string errorMsg = "get last locator failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "builder locator [%s]", locator.toDebugString().c_str());
    return true;
}

bool DocBuilderConsumer::stop(FlowError lastError) {
    if (lastError == FE_EOF) {
        _builder->stop(_endTimestamp);
    } else {
        _builder->stop();
    }
    return !_builder->hasFatalError();
}

}
}
