#ifndef ISEARCH_BS_DOCPROCESSORPRODUCER_H
#define ISEARCH_BS_DOCPROCESSORPRODUCER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/processor/Processor.h"
namespace build_service {
namespace workflow {

class DocProcessorProducer : public ProcessedDocProducer
{
public:
    DocProcessorProducer(processor::Processor *processor);
    ~DocProcessorProducer();
private:
    DocProcessorProducer(const DocProcessorProducer &);
    DocProcessorProducer& operator=(const DocProcessorProducer &);
public:
    /* override */ FlowError produce(document::ProcessedDocumentVecPtr &processedDocVec);
    /* override */ bool seek(const common::Locator &locator);
    /* override */ bool stop();
private:
    processor::Processor *_processor;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocProcessorProducer);

}
}

#endif //ISEARCH_BS_DOCPROCESSORPRODUCER_H
