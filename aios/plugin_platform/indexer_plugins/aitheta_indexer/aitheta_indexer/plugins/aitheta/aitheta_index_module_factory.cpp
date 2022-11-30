#include "aitheta_indexer/plugins/aitheta/aitheta_index_module_factory.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_reducer.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_retriever.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_index_segment_retriever.h"
#include "aitheta_indexer/plugins/aitheta/aitheta_indexer.h"

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(aitheta_plugin);

void AithetaIndexModuleFactory::destroy() { delete this; }
IndexRetriever *AithetaIndexModuleFactory::createIndexRetriever(const KeyValueMap &parameters) {
    return new AithetaIndexRetriever(parameters);
}

Indexer *AithetaIndexModuleFactory::createIndexer(const KeyValueMap &parameters) {
    return new AithetaIndexer(parameters);
}

IndexReducer *AithetaIndexModuleFactory::createIndexReducer(const KeyValueMap &parameters) {
    return new AithetaIndexReducer(parameters);
}

IndexSegmentRetriever *AithetaIndexModuleFactory::createIndexSegmentRetriever(const KeyValueMap &parameters) {
    return new AithetaIndexSegmentRetriever(parameters);
}

extern "C" plugin::ModuleFactory *createFactory_Index() { return new AithetaIndexModuleFactory(); }

extern "C" void destroyFactory_Index(plugin::ModuleFactory *factory) { factory->destroy(); }

IE_NAMESPACE_END(aitheta_plugin);
