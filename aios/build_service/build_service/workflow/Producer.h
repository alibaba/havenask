#ifndef ISEARCH_BS_PRODUCER_H
#define ISEARCH_BS_PRODUCER_H

#include <indexlib/document/document_factory_wrapper.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/FlowError.h"
#include "build_service/common/Locator.h"
#include "build_service/document/RawDocument.h"

namespace build_service {
namespace document {

class ProcessedDocument;
BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);

}
}

namespace build_service {
namespace workflow {

template <typename T>
class Producer
{
public:
    Producer() {
    }
    virtual ~Producer() {
    }
private:
    Producer(const Producer &);
    Producer& operator=(const Producer &);
public:
    virtual FlowError produce(T &item) = 0;
    virtual bool seek(const common::Locator &locator) = 0;
    virtual bool stop() = 0;

protected:
    IE_NAMESPACE(document)::DocumentFactoryWrapperPtr _documentFactoryWrapper;
};

typedef Producer<document::RawDocumentPtr> RawDocProducer;
typedef Producer<document::ProcessedDocumentVecPtr> ProcessedDocProducer;

}
}

#endif //ISEARCH_BS_PRODUCER_H
