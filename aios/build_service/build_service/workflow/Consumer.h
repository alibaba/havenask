#ifndef ISEARCH_BS_CONSUMER_H
#define ISEARCH_BS_CONSUMER_H

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
class Consumer
{
public:
    Consumer() {
    }
    virtual ~Consumer() {
    }
private:
    Consumer(const Consumer &);
    Consumer& operator=(const Consumer &);
public:
    virtual FlowError consume(const T &item) = 0;
    virtual bool getLocator(common::Locator &locator) const = 0;
    virtual bool stop(FlowError lastError) = 0;
};

typedef Consumer<document::RawDocumentPtr> RawDocConsumer;
typedef Consumer<document::ProcessedDocumentVecPtr> ProcessedDocConsumer;

}
}

#endif //ISEARCH_BS_CONSUMER_H
