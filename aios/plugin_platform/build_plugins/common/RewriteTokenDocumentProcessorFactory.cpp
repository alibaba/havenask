#include "RewriteTokenDocumentProcessorFactory.h"

using namespace std;

namespace pluginplatform {
namespace processor_plugins {

bool RewriteTokenDocumentProcessorFactory::init(const build_service::KeyValueMap& parameters)
{
    return true;
}

void RewriteTokenDocumentProcessorFactory::destroy()
{
    delete this;
}
 
RewriteTokenDocumentProcessor* RewriteTokenDocumentProcessorFactory::createDocumentProcessor(
        const string& processorName)
{
    RewriteTokenDocumentProcessor* processor = new RewriteTokenDocumentProcessor;
    return processor;
}


extern "C"
build_service::util::ModuleFactory* createFactory() 
{
    return new RewriteTokenDocumentProcessorFactory;
}

extern "C"
void destroyFactory(build_service::util::ModuleFactory* factory) 
{
    factory->destroy();
}

}}
