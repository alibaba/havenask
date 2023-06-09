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

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/RangeUtil.h"
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class FilterClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

class SearcherCacheClause : public ClauseBase
{
public:
    static const uint32_t INVALID_TIME = uint32_t(-1);
public:
    SearcherCacheClause();
    ~SearcherCacheClause();
private:
    SearcherCacheClause(const SearcherCacheClause &);
    SearcherCacheClause& operator=(const SearcherCacheClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setKey(uint64_t key) { _key = key; }
    uint64_t getKey() const { return _key; }
    void setPartRange(autil::PartitionRange partRange) { _partRange = partRange; }
    autil::PartitionRange getPartRange() const { return _partRange; }
    void setUse(bool use) { _use = use; }
    bool getUse() const { return _use; }
    void setCurrentTime(uint32_t curTime) { _currentTime = curTime; }
    uint32_t getCurrentTime() const { return _currentTime; }
    void setFilterClause(FilterClause *filterClause);
    FilterClause* getFilterClause() const { return _filterClause;}
    void setExpireExpr(suez::turing::SyntaxExpr *syntaxExpr);
    suez::turing::SyntaxExpr* getExpireExpr() const { return _expireExpr; }
    const std::vector<uint32_t>& getCacheDocNumVec() const{ return _cacheDocNumVec; }
    void setCacheDocNumVec(const std::vector<uint32_t>& cacheDocNumVec) {
        _cacheDocNumVec = cacheDocNumVec;
    }
    void setRefreshAttributes(const std::set<std::string> &refreshAttribute) {
        _refreshAttributes = refreshAttribute;
    }
    const std::set<std::string>& getRefreshAttributes() const {
        return _refreshAttributes;
    }
private:
    bool _use;
    uint32_t _currentTime;//s
    uint64_t _key;
    autil::PartitionRange _partRange;
    FilterClause *_filterClause;
    suez::turing::SyntaxExpr *_expireExpr;
    std::vector<uint32_t> _cacheDocNumVec;
    std::set<std::string> _refreshAttributes;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherCacheClause> SearcherCacheClausePtr;

} // namespace common
} // namespace isearch
