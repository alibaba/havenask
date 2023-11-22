#pragma once

#include <memory>

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/customized_index/in_mem_segment_retriever.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class DemoInMemSegmentRetriever : public InMemSegmentRetriever
{
public:
    typedef std::pair<docid_t, std::string> DocItem;
    typedef autil::mem_pool::pool_allocator<std::pair<const docid_t, std::string>> AllocatorType;
    typedef std::map<docid_t, std::string, std::less<docid_t>, AllocatorType> DataMap;

public:
    DemoInMemSegmentRetriever(const DataMap* dataMap, const util::KeyValueMap& parameters)
        : InMemSegmentRetriever(parameters)
        , mDataMap(dataMap)
    {
    }

    ~DemoInMemSegmentRetriever() {}

public:
    MatchInfo Search(const std::string& query, autil::mem_pool::Pool* sessionPool) override;

private:
    const DataMap* mDataMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DemoInMemSegmentRetriever);
}} // namespace indexlib::index
