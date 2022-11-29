#ifndef __INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H
#define __INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H

#include <indexlib/index/normal/inverted_index/customized_index/index_module_factory.h>

IE_NAMESPACE_BEGIN(aitheta_plugin);

class AithetaIndexModuleFactory : public IE_NAMESPACE(index)::IndexModuleFactory {
 public:
    AithetaIndexModuleFactory() = default;
    ~AithetaIndexModuleFactory() = default;

 public:
    void destroy();
    IE_NAMESPACE(index)::IndexSegmentRetriever *createIndexSegmentRetriever(const util::KeyValueMap &parameters);
    IE_NAMESPACE(index)::Indexer *createIndexer(const util::KeyValueMap &parameters);
    IE_NAMESPACE(index)::IndexReducer *createIndexReducer(const util::KeyValueMap &parameters);
    IE_NAMESPACE(index)::IndexRetriever *createIndexRetriever(const util::KeyValueMap &parameters);

 private:
    util::KeyValueMap _parameters;
};

DEFINE_SHARED_PTR(AithetaIndexModuleFactory);

IE_NAMESPACE_END(aitheta_plugin);

#endif  //__INDEXLIB_AITHETA_INDEX_MODULE_FACTORY_H
