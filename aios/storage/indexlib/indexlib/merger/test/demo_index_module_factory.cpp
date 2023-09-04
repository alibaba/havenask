#include "indexlib/merger/test/demo_index_module_factory.h"

#include "indexlib/merger/test/demo_index_reducer.h"
#include "indexlib/merger/test/demo_index_segment_retriever.h"
#include "indexlib/merger/test/demo_indexer.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace merger {

DemoIndexModuleFactory::DemoIndexModuleFactory() {}

DemoIndexModuleFactory::~DemoIndexModuleFactory() {}

void DemoIndexModuleFactory::destroy() { delete this; }

Indexer* DemoIndexModuleFactory::createIndexer(const KeyValueMap& parameters) { return new DemoIndexer(parameters); }

IndexReducer* DemoIndexModuleFactory::createIndexReducer(const KeyValueMap& parameters)
{
    return new DemoIndexReducer(parameters);
}

IndexSegmentRetriever* DemoIndexModuleFactory::createIndexSegmentRetriever(const KeyValueMap& parameters)
{
    return new DemoIndexSegmentRetriever(parameters);
}

extern "C" plugin::ModuleFactory* createFactory_Index() { return new DemoIndexModuleFactory(); }

extern "C" void destroyFactory_Index(plugin::ModuleFactory* factory) { factory->destroy(); }
}} // namespace indexlib::merger
