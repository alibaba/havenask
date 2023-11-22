#pragma once

#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace merger {

class DemoIndexer : public index::Indexer
{
private:
    typedef std::pair<docid_t, std::string> DocItem;
    typedef autil::mem_pool::pool_allocator<std::pair<const docid_t, std::string>> AllocatorType;
    typedef std::map<docid_t, std::string, std::less<docid_t>, AllocatorType> DataMap;

public:
    static const std::string DATA_FILE_NAME;
    static const std::string META_FILE_NAME;
    typedef std::map<std::string, std::string> KVMap;

public:
    DemoIndexer(const util::KeyValueMap& parameters);
    ~DemoIndexer();

public:
    bool DoInit(const IndexerResourcePtr& resource) override;
    bool Build(const std::vector<const document::Field*>& fieldVec, docid_t docId) override;
    bool Dump(const file_system::DirectoryPtr& indexDir) override;

    size_t EstimateInitMemoryUse(size_t lastSegmentDistinctTermCount) const override;
    int64_t EstimateTempMemoryUseInDump() const override;
    int64_t EstimateDumpFileSize() const override;

private:
    DataMap mDataMap;

    int64_t mTotalDocStrLen;
    static constexpr float TO_JSON_EXPAND_FACTOR = 1.5;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoIndexer);
}} // namespace indexlib::merger
