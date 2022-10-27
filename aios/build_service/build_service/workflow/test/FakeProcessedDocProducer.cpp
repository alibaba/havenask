#include <indexlib/document/index_document/normal_document/normal_document.h>
#include "build_service/workflow/test/FakeProcessedDocProducer.h"
#include "build_service/document/ProcessedDocument.h"

using namespace std;
using namespace build_service::document;
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(config);

namespace build_service {
namespace workflow {
BS_LOG_SETUP(workflow, FakeProcessedDocProducer);

FakeProcessedDocProducer::FakeProcessedDocProducer(uint32_t docCount) 
    : SwiftProcessedDocProducer(NULL, IndexPartitionSchemaPtr(), "")
    , _totalDocCount(docCount)
    , _idx(0)
    , _sleep(0)
{
    sleep(_sleep);
}

FakeProcessedDocProducer::~FakeProcessedDocProducer() {
}


FlowError FakeProcessedDocProducer::produce(
        ProcessedDocumentVecPtr &processedDocVec)
{
    if (_idx >= _totalDocCount)
    {
        return FE_EOF;
    }

    ProcessedDocumentVecPtr docVec(new ProcessedDocumentVec);
    ProcessedDocumentPtr doc(new ProcessedDocument);
    doc->setLocator(common::Locator(_idx++));
    doc->setDocument(DocumentPtr(new NormalDocument));
    docVec->push_back(doc);
    processedDocVec = docVec;
    return FE_OK;
}

bool FakeProcessedDocProducer::seek(const common::Locator &locator) {
    if (locator.getOffset() > (int64_t)_totalDocCount)
    {
        return false;
    }
    _idx = (uint32_t)locator.getOffset();
    return true;
}

bool FakeProcessedDocProducer::stop() {
    return true;
}

bool FakeProcessedDocProducer::getMaxTimestamp(int64_t &timestamp) {
    timestamp = _totalDocCount;
    return true;
}

}
}
