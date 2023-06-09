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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/isearch.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class FinalSortClause : public ClauseBase
{
public:
    static const std::string DEFAULT_SORTER;
public:
    FinalSortClause();
    ~FinalSortClause();
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
private:
    FinalSortClause(const FinalSortClause &);
    FinalSortClause& operator=(const FinalSortClause &);
public:
    const std::string& getSortName() const {
        return _sortName;
    }
    void setSortName(const std::string& sortName) {
        _sortName = sortName;
    }
    bool useDefaultSort() const {
        return _sortName == DEFAULT_SORTER;
    }
    void addParam(const std::string& key, const std::string& value) {
        _params[key] = value;
    }
    const KeyValueMap& getParams() const {
        return _params;
    }
private:
    std::string _sortName;
    KeyValueMap _params;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FinalSortClause> FinalSortClausePtr;

} // namespace common
} // namespace isearch

