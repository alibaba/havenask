#ifndef __INDEXLIB_DEMO_INDEX_MODULE_FACTORY_H
#define __INDEXLIB_DEMO_INDEX_MODULE_FACTORY_H

#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"

IE_NAMESPACE_BEGIN(index);

class DemoIndexModuleFactory : public IndexModuleFactory
{
public:
    DemoIndexModuleFactory();
    ~DemoIndexModuleFactory();
public:
    void destroy() override;
    Indexer* createIndexer(const util::KeyValueMap &parameters) override;
    IndexReducer* createIndexReducer(const util::KeyValueMap &parameters) override;
    IndexSegmentRetriever* createIndexSegmentRetriever(const util::KeyValueMap &parameters) override;
    IndexRetriever* createIndexRetriever(const util::KeyValueMap& parameters) override;
    IndexerUserData* createIndexerUserData(const util::KeyValueMap& parameters) override;

private:
    util::KeyValueMap _parameters;
};

DEFINE_SHARED_PTR(DemoIndexModuleFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEMO_INDEX_MODULE_FACTORY_H
