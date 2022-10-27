#include "build_service/processor/test/SimpleDocumentProcessorFactory.h"
#include "build_service/processor/test/SimpleDocumentProcessor.h"
#include "build_service/processor/test/EraseFieldProcessor.h"
#include "build_service/processor/test/ReusedDocumentProcessor.h"

using namespace std;

namespace build_service {
namespace processor {

bool SimpleDocumentProcessorFactory::init(const KeyValueMap& parameters)
{
    return true;
}

void SimpleDocumentProcessorFactory::destroy()
{
    delete this;
}

DocumentProcessor* SimpleDocumentProcessorFactory::createDocumentProcessor(
        const string& processorName)
{
    if (processorName == "SimpleDocumentProcessor") {
        return new SimpleDocumentProcessor;
    } else if (processorName == "EraseFieldProcessor") {
        return new EraseFieldProcessor;
    } else if (processorName == "ReusedDocumentProcessor") {
        return new ReusedDocumentProcessor;
    }
    return NULL;
}


extern "C"
IE_NAMESPACE(plugin)::ModuleFactory* createFactory()
{
    return new SimpleDocumentProcessorFactory();
}

extern "C"
void destroyFactory(IE_NAMESPACE(plugin)::ModuleFactory* factory)
{
    factory->destroy();
}



}
}
