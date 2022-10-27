#include "indexlib/util/cache/search_cache_partition_wrapper.h"
#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <autil/MurmurHash.h>

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, SearchCachePartitionWrapper);

SearchCachePartitionWrapper::SearchCachePartitionWrapper(const SearchCachePtr& searchCache,
                                                         uint64_t partitionId)
    : mSearchCache(searchCache), mPartitionId(partitionId) {
    std::array<uint64_t, 2> buffer;
    buffer[0] = partitionId;
    buffer[1] = autil::TimeUtility::currentTime();
    mPartitionId =
        autil::MurmurHash::MurmurHash64A(buffer.data(), sizeof(uint64_t) * buffer.size(), 1);
}

SearchCachePartitionWrapper::~SearchCachePartitionWrapper() 
{
}

IE_NAMESPACE_END(util);

