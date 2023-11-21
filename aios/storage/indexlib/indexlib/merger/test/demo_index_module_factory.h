#pragma once

#include "indexlib/index/normal/inverted_index/customized_index/index_module_factory.h"

namespace indexlib { namespace merger {

class DemoIndexModuleFactory : public index::IndexModuleFactory
{
public:
    DemoIndexModuleFactory();
    ~DemoIndexModuleFactory();

public:
    void destroy() override;
    index::Indexer* createIndexer(const util::KeyValueMap& parameters) override;

    index::IndexReducer* createIndexReducer(const util::KeyValueMap& parameters) override;

    index::IndexSegmentRetriever* createIndexSegmentRetriever(const util::KeyValueMap& parameters) override;

private:
    util::KeyValueMap _parameters;
};

DEFINE_SHARED_PTR(DemoIndexModuleFactory);
}} // namespace indexlib::merger
