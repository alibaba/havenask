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

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class SortDescription;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class RankSortDescription
{
public:
    RankSortDescription();
    ~RankSortDescription();
private:
    RankSortDescription(const RankSortDescription &);
    RankSortDescription& operator=(const RankSortDescription &);
public:
    void setSortDescs(const std::vector<SortDescription*> &sortDesc);
    size_t getSortDescCount() const {
        return _sortDescs.size();
    }
    const SortDescription* getSortDescription(size_t idx) const {
        assert(idx < _sortDescs.size());
        return _sortDescs[idx];
    }

    const std::vector<SortDescription*> &getSortDescriptions() const {
        return _sortDescs;
    }

    void setPercent(const std::string &percentStr);
    float getPercent() const {
        return _percent;
    }
    std::string toString() const;
public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
private:
    std::vector<SortDescription*> _sortDescs;
    float _percent;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RankSortDescription> RankSortDescriptionPtr;

} // namespace common
} // namespace isearch

