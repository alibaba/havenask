#include "indexlib/merger/test/demo_index_module_factory.h"
#include "indexlib/merger/test/demo_indexer.h"
#include "indexlib/merger/test/demo_index_segment_retriever.h"
#include "indexlib/merger/test/demo_index_reducer.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);

DemoIndexModuleFactory::DemoIndexModuleFactory() 
{
}

DemoIndexModuleFactory::~DemoIndexModuleFactory() 
{
}

void DemoIndexModuleFactory::destroy()
{
    delete this;
}

Indexer* DemoIndexModuleFactory::createIndexer(
        const KeyValueMap &parameters)
{
    return new DemoIndexer(parameters);
}

IndexReducer* DemoIndexModuleFactory::createIndexReducer(
        const KeyValueMap &parameters)
{
    return new DemoIndexReducer(parameters);
}

IndexSegmentRetriever* DemoIndexModuleFactory::createIndexSegmentRetriever(
        const KeyValueMap &parameters)
{
    return new DemoIndexSegmentRetriever(parameters);
}

extern "C"
plugin::ModuleFactory* createFactory_Index() {
    return new DemoIndexModuleFactory();
}

extern "C"
void destroyFactory_Index(plugin::ModuleFactory *factory) {
    factory->destroy();
}

IE_NAMESPACE_END(merger);

