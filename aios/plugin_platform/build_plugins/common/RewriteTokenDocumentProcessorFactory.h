#include <ha3/common.h>
#include <tr1/memory>
#include <build_service/processor/DocumentProcessorFactory.h>
#include "processor_demo/RewriteTokenDocumentProcessor.h"

namespace pluginplatform {
namespace processor_plugins {

class RewriteTokenDocumentProcessorFactory : public build_service::processor::DocumentProcessorFactory
{
public:
    virtual ~RewriteTokenDocumentProcessorFactory() {}
public:
    virtual bool init(const build_service::KeyValueMap& parameters);
    virtual void destroy();
    virtual RewriteTokenDocumentProcessor* createDocumentProcessor(const std::string& processorName);
};

typedef std::tr1::shared_ptr<RewriteTokenDocumentProcessorFactory> RewriteTokenDocumentProcessorFactoryPtr;

}}
