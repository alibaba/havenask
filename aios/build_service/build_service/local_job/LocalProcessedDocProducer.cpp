#include "build_service/local_job/LocalProcessedDocProducer.h"
#include "build_service/document/ProcessedDocument.h"

using namespace std;
using namespace build_service::workflow;
using namespace build_service::document;
IE_NAMESPACE_USE(document);

namespace build_service {
namespace local_job {

BS_LOG_SETUP(local_job, LocalProcessedDocProducer);

LocalProcessedDocProducer::LocalProcessedDocProducer(
        ReduceDocumentQueue *queue, uint64_t src, uint16_t instanceId)
    : _queue(queue)
    , _src(src)
    , _instanceId(instanceId)
    , _curDocCount(0)
{
}

LocalProcessedDocProducer::~LocalProcessedDocProducer() {
}

FlowError LocalProcessedDocProducer::produce(
        ProcessedDocumentVecPtr &docVec)
{
    DocumentPtr document;
    if (!_queue->pop(_instanceId, document)) {
        BS_LOG(INFO, "finish read all record! total record count[%d]",
               _curDocCount);
        return FE_EOF;
    }

    docVec.reset(new ProcessedDocumentVec());
    ProcessedDocumentPtr processedDoc(new ProcessedDocument);
    processedDoc->setLocator(common::Locator(_src, ++_curDocCount));
    processedDoc->setDocument(document);
    docVec->push_back(processedDoc);
    return FE_OK;
}

bool LocalProcessedDocProducer::seek(const common::Locator &locator) {
    return true;
}

bool LocalProcessedDocProducer::stop() {
    return true;
}

}
}
