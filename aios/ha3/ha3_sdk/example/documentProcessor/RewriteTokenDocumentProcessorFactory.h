#ifndef __REWRITE_TOKEN_DOCUMENT_PROCESSOR_FACTORY_H
#define __REWRITE_TOKEN_DOCUMENT_PROCESSOR_FACTORY_H

#include <ha3/common.h>
#include <tr1/memory>
#include <build_service/processor/DocumentProcessorFactory.h>
#include "RewriteTokenDocumentProcessor.h"

namespace build_service {
namespace processor {

class RewriteTokenDocumentProcessorFactory : public DocumentProcessorFactory
{
public:
    virtual ~RewriteTokenDocumentProcessorFactory() {}
public:
    virtual bool init(const KeyValueMap& parameters);
    virtual void destroy();
    virtual RewriteTokenDocumentProcessor* createDocumentProcessor(const std::string& processorName);
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessorFactory> RewriteTokenDocumentProcessorFactoryPtr;

};
};

#endif //__BUILD_DOCUMENT_PROCESSOR_FACTORY_H
