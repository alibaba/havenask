#include "RewriteTokenDocumentProcessorFactory.h"

using namespace std;

namespace build_service {
namespace processor {

bool RewriteTokenDocumentProcessorFactory::init(const KeyValueMap& parameters)
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
util::ModuleFactory* createFactory() 
{
    return new RewriteTokenDocumentProcessorFactory;
}

extern "C"
void destroyFactory(util::ModuleFactory* factory) 
{
    factory->destroy();
}

};
};
