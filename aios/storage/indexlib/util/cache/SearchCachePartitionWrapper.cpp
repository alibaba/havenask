/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/util/cache/SearchCachePartitionWrapper.h"

#include <array>

#include "autil/MurmurHash.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, SearchCachePartitionWrapper);

SearchCachePartitionWrapper::SearchCachePartitionWrapper(const SearchCachePtr& searchCache, const string& partitionName)
    : _searchCache(searchCache)
    , _partitionId(0)
    , _partitionName(partitionName)
    , _exclusiveId("")
    , _timestamp(autil::TimeUtility::currentTime())
{
    InitPartitionId();
}

SearchCachePartitionWrapper::~SearchCachePartitionWrapper() {}

SearchCachePartitionWrapper* SearchCachePartitionWrapper::CreateExclusiveCacheWrapper(const string& id)
{
    auto ret = make_unique<SearchCachePartitionWrapper>(*this);
    ret->_exclusiveId = id;
    ret->InitPartitionId();

    AUTIL_LOG(INFO,
              "create exclusive cache wrapper: "
              "id [%s] partition [%s] timestamp [%lu], final partitionId [%lu]",
              id.c_str(), _partitionName.c_str(), _timestamp, ret->_partitionId);
    return ret.release();
}

void SearchCachePartitionWrapper::InitPartitionId()
{
    std::array<uint64_t, 3> buffer;
    buffer[0] = autil::MurmurHash::MurmurHash64A(_partitionName.data(), _partitionName.length(), 0);
    buffer[1] = _timestamp;
    buffer[2] = autil::MurmurHash::MurmurHash64A(_exclusiveId.data(), _exclusiveId.length(), 0);
    _partitionId = autil::MurmurHash::MurmurHash64A(buffer.data(), sizeof(uint64_t) * buffer.size(), 1);
}

}} // namespace indexlib::util
