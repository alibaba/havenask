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

#include <memory>
#include <set>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil

namespace isearch {
namespace common {

class AttributeClause : public ClauseBase
{
public:
    AttributeClause();
    ~AttributeClause();
private:
    AttributeClause(const AttributeClause &);
    AttributeClause& operator=(const AttributeClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    const std::set<std::string>& getAttributeNames() const {
        return _attributeNames;
    }

    std::set<std::string>& getAttributeNames() {
        return _attributeNames;
    }

    void addAttribute(const std::string &name) {
        _attributeNames.insert(name);
    }

    template<typename Iterator>
    void setAttributeNames(Iterator begin, Iterator end) {
        _attributeNames.clear();
        _attributeNames.insert(begin, end);
    }
private:
    std::set<std::string> _attributeNames;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeClause> AttributeClausePtr;

} // namespace common
} // namespace isearch

