#ifndef __BUILD_DOCUMENT_PROCESSOR_FACTORY_H
#define __BUILD_DOCUMENT_PROCESSOR_FACTORY_H

#include <tr1/memory>
#include "build_service/processor/DocumentProcessorFactory.h"

namespace build_service {
namespace processor {

class SimpleDocumentProcessorFactory : public DocumentProcessorFactory
{
public:
    virtual ~SimpleDocumentProcessorFactory() {}
public:
    /* override */ bool init(const KeyValueMap& parameters);
    /* override */ void destroy();
    /* override */ DocumentProcessor* createDocumentProcessor(const std::string& processorName);
};

typedef std::tr1::shared_ptr<SimpleDocumentProcessorFactory> SimpleDocumentProcessorFactoryPtr;

}
}

#endif //__BUILD_DOCUMENT_PROCESSOR_FACTORY_H
