#pragma once

#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"

namespace indexlib { namespace index {

class DemoIndexModuleFactory : public IndexModuleFactory
{
public:
    DemoIndexModuleFactory();
    ~DemoIndexModuleFactory();

public:
    void destroy() override;
    Indexer* createIndexer(const util::KeyValueMap& parameters) override;
    IndexReducer* createIndexReducer(const util::KeyValueMap& parameters) override;
    IndexSegmentRetriever* createIndexSegmentRetriever(const util::KeyValueMap& parameters) override;
    IndexRetriever* createIndexRetriever(const util::KeyValueMap& parameters) override;
    IndexerUserData* createIndexerUserData(const util::KeyValueMap& parameters) override;

private:
    util::KeyValueMap _parameters;
};

DEFINE_SHARED_PTR(DemoIndexModuleFactory);
}} // namespace indexlib::index
