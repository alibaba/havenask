#include "build_service/workflow/RawDocBuilderConsumer.h"
#include "build_service/document/ProcessedDocument.h"
using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(common);
namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, RawDocBuilderConsumer);

RawDocBuilderConsumer::RawDocBuilderConsumer(builder::Builder *builder,
                                             processor::Processor *processor)
    : _builder(builder)
    , _processor(processor)
    , _endTimestamp(INVALID_TIMESTAMP)
{
}

RawDocBuilderConsumer::~RawDocBuilderConsumer() {
}

FlowError RawDocBuilderConsumer::consume(const document::RawDocumentPtr &item) {
    if (unlikely(!item)) {
        return FE_OK;
    }
    BS_LOG(DEBUG, "build locator is: %s", item->getLocator().toDebugString().c_str());
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }

    if (_processor) {
        return processAndBuildDoc(item);
    } else {
        return buildDoc(item);
    }

    return FE_EXCEPTION;
}

FlowError RawDocBuilderConsumer::processAndBuildDoc(const document::RawDocumentPtr &item) {
    assert(_processor);
    _processor->processDoc(item);
    auto processedDocVecPtr = _processor->getProcessedDoc();
    if (processedDocVecPtr == nullptr || processedDocVecPtr->empty()) {
        return FE_OK;
    }
    for (size_t i = 0; i < processedDocVecPtr->size(); ++i) {
        const ProcessedDocumentPtr &processedDoc = (*processedDocVecPtr)[i];
        const ProcessedDocument::DocClusterMetaVec &metaVec =
            processedDoc->getDocClusterMetaVec();
        const DocumentPtr &document = processedDoc->getDocument();
        const common::Locator &locator = processedDoc->getLocator();
        if (!document || document->GetDocOperateType() == SKIP_DOC
            || document->GetDocOperateType() == UNKNOWN_OP)
        {
            continue;
        }
        if (!_builder->build(document) || _builder->hasFatalError()) {
            return FE_FATAL;
        }
    }
    return FE_OK;
}

FlowError RawDocBuilderConsumer::buildDoc(const document::RawDocumentPtr &item) {
    if (_builder->build(item)) {
        return FE_OK;
    }
    if (_builder->hasFatalError()) {
        return FE_FATAL;
    }

    return FE_EXCEPTION;
}

bool RawDocBuilderConsumer::getLocator(common::Locator &locator) const {
    if (!_builder->getLastLocator(locator)) {
        string errorMsg = "get last locator failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    BS_LOG(INFO, "builder locator [%s]", locator.toDebugString().c_str());
    return true;
}

bool RawDocBuilderConsumer::stop(FlowError lastError) {
    if (lastError == FE_EOF) {
        _builder->stop(_endTimestamp);
    } else {
        _builder->stop();
    }
    return !_builder->hasFatalError();
}

}
}
