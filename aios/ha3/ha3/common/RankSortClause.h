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

#include <assert.h>
#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class RankSortDescription;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class RankSortClause : public ClauseBase
{
public:
    RankSortClause();
    ~RankSortClause();
private:
    RankSortClause(const RankSortClause &);
    RankSortClause& operator=(const RankSortClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void addRankSortDesc(RankSortDescription* desc) {
        _descs.push_back(desc);
    }
    size_t getRankSortDescCount() const {
        return _descs.size();
    }
    const RankSortDescription* getRankSortDesc(size_t idx) const {
        assert (idx < _descs.size());
        return _descs[idx];
    }
private:
    std::vector<RankSortDescription*> _descs;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RankSortClause> RankSortClausePtr;

} // namespace common
} // namespace isearch

