#ifndef __INDEXLIB_INDEX_MODULE_FACTORY_H
#define __INDEXLIB_INDEX_MODULE_FACTORY_H

#include "indexlib/plugin/ModuleFactory.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_segment_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_retriever.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_user_data.h"

IE_NAMESPACE_BEGIN(index);

class IndexModuleFactory : public plugin::ModuleFactory
{
public:
    IndexModuleFactory() {};
    ~IndexModuleFactory() {};
public:
    virtual void destroy() = 0;    
    virtual Indexer* createIndexer(const util::KeyValueMap& parameters) = 0;
    virtual IndexReducer* createIndexReducer(const util::KeyValueMap& parameters) = 0;
    virtual IndexSegmentRetriever* createIndexSegmentRetriever(const util::KeyValueMap& parameters) = 0;
    virtual IndexRetriever* createIndexRetriever(const util::KeyValueMap& parameters)
    {
        return new IndexRetriever();
    }
    
    virtual IndexerUserData* createIndexerUserData(const util::KeyValueMap& parameters)
    {
        return new IndexerUserData();
    }
};

DEFINE_SHARED_PTR(IndexModuleFactory);

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_INDEX_MODULE_FACTORY_H
