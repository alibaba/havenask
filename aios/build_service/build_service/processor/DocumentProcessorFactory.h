#ifndef ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H
#define ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H

#include <tr1/memory>
#include "build_service/processor/DocumentProcessor.h"
#include "build_service/plugin/ModuleFactory.h"

namespace build_service {
namespace processor {

class DocumentProcessorFactory : public plugin::ModuleFactory
{
public:
    DocumentProcessorFactory() {}
    virtual ~DocumentProcessorFactory() {}
public:
    virtual bool init(const KeyValueMap &parameters) = 0;
    virtual void destroy() = 0;

    virtual DocumentProcessor* createDocumentProcessor(const std::string &processorName) = 0;
private:
};

typedef std::tr1::shared_ptr<DocumentProcessorFactory> DocumentProcessorFactoryPtr;

}
}

#endif //ISEARCH_BS_DOCUMENT_PROCESSOR_MODULE_FACTORY_H
