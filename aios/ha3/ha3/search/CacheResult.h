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
#pragma once

#include <stdint.h>
#include <algorithm>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/CacheMinScoreFilter.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace autil {
class DataBuffer;

namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class Result;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

class CacheHeader {
public:
    CacheHeader() {
        expireTime = INVALID_SEARCHER_CACHE_EXPIRE_TIME;
        time = 0;
    }
public:
    uint32_t expireTime; // s
    CacheMinScoreFilter minScoreFilter;
    uint32_t time; // s
};

class CacheResult
{
public:
    CacheResult();
    ~CacheResult();
private:
    CacheResult(const CacheResult &);
    CacheResult& operator=(const CacheResult &);
public:
    const CacheHeader* getHeader() const { return &_header; }
    CacheHeader* getHeader() { return &_header; }

    void setPartInfoHint(const indexlib::index::PartitionInfoHint &partInfoHint) {
        _partInfoHint = partInfoHint;
    }
    const indexlib::index::PartitionInfoHint& getPartInfoHint() const { return _partInfoHint; }

    const std::vector<globalid_t>& getGids() const { return _gids; }
    std::vector<globalid_t>& getGids() { return _gids; }

    const std::vector<globalid_t>& getSubDocGids() const { return _subDocGids; }
    std::vector<globalid_t>& getSubDocGids() { return _subDocGids; }

    void setResult(common::Result *result) { _result = result; }
    common::Result* getResult() const { return _result; }
    common::Result* stealResult();

    bool isTruncated() const { return _isTruncated; }
    void setTruncated(bool isTruncated) {_isTruncated = isTruncated;}

    bool useTruncateOptimizer() const { return _useTruncateOptimizer; }
    void setUseTruncateOptimizer(bool flag) { _useTruncateOptimizer = flag; }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void serializeHeader(autil::DataBuffer &dataBuffer) const;
    void serializeResult(autil::DataBuffer &dataBuffer) const;

    void deserialize(autil::DataBuffer &dataBuffer,
                     autil::mem_pool::Pool *pool);
    void deserializeHeader(autil::DataBuffer &dataBuffer);
    void deserializeResult(autil::DataBuffer &dataBuffer,
                           autil::mem_pool::Pool *pool);
public:
    // for test
    void setGids(const std::vector<globalid_t> &gids)  { _gids = gids; }
private:
    CacheHeader _header;
    common::Result *_result;
    bool _isTruncated;
    bool _useTruncateOptimizer;
    indexlib::index::PartitionInfoHint _partInfoHint;
    std::vector<globalid_t> _gids;
    std::vector<globalid_t> _subDocGids;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CacheResult> CacheResultPtr;

} // namespace search
} // namespace isearch

