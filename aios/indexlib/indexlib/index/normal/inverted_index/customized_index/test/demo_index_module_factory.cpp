#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_module_factory.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_indexer.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_index_reducer.h"
#include "indexlib/index/normal/inverted_index/customized_index/test/demo_user_data.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

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
        const util::KeyValueMap &parameters)
{
    return new DemoIndexer(parameters);
}

IndexReducer* DemoIndexModuleFactory::createIndexReducer(
        const util::KeyValueMap &parameters)
{
    return new DemoIndexReducer(parameters);
}

IndexSegmentRetriever* DemoIndexModuleFactory::createIndexSegmentRetriever(
        const util::KeyValueMap &parameters)
{
    return new DemoIndexSegmentRetriever(parameters);
}

IndexRetriever* DemoIndexModuleFactory::createIndexRetriever(const util::KeyValueMap& parameters)
{
    return new DemoIndexRetriever();
}

IndexerUserData* DemoIndexModuleFactory::createIndexerUserData(const util::KeyValueMap& parameters)
{
    return new DemoUserData();
}

extern "C"
plugin::ModuleFactory* createFactory_Index() {
    return new DemoIndexModuleFactory();
}

extern "C"
void destroyFactory_Index(plugin::ModuleFactory *factory) {
    factory->destroy();
}

IE_NAMESPACE_END(index);

