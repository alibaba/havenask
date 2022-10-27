#ifndef ISEARCH_BS_DOCPROCESSORCONSUMER_H
#define ISEARCH_BS_DOCPROCESSORCONSUMER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Consumer.h"
#include "build_service/processor/Processor.h"

namespace build_service {
namespace workflow {

class DocProcessorConsumer : public RawDocConsumer
{
public:
    DocProcessorConsumer(processor::Processor *processor);
    ~DocProcessorConsumer();
private:
    DocProcessorConsumer(const DocProcessorConsumer &);
    DocProcessorConsumer& operator=(const DocProcessorConsumer &);
public:
    /* override */ FlowError consume(const document::RawDocumentPtr &item);
    /* override */ bool getLocator(common::Locator &locator) const;
    /* override */ bool stop(FlowError lastError);
private:
    processor::Processor *_processor;
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_DOCPROCESSORCONSUMER_H
