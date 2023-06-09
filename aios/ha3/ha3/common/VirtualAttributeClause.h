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

#include <stddef.h>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace common {

class VirtualAttributeClause : public ClauseBase
{
public:
    VirtualAttributeClause();
    ~VirtualAttributeClause();
private:
    VirtualAttributeClause(const VirtualAttributeClause &);
    VirtualAttributeClause& operator=(const VirtualAttributeClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    bool addVirtualAttribute(const std::string &name, 
                             suez::turing::SyntaxExpr *syntaxExpr);
    size_t getVirtualAttributeSize() const {return _virtualAttributes.size();}
    const std::vector<suez::turing::VirtualAttribute*>& getVirtualAttributes() const {
        return _virtualAttributes;}
    suez::turing::VirtualAttribute* getVirtualAttribute(const std::string &name) const {
        std::vector<suez::turing::VirtualAttribute*>::const_iterator it = 
            _virtualAttributes.begin();
        for (; it != _virtualAttributes.end(); ++it) {
            if (name == (*it)->getVirtualAttributeName()) {
                return (*it);
            }
        }
        return NULL;
    }
private:
    std::vector<suez::turing::VirtualAttribute*> _virtualAttributes;
private:
    friend class VirtualAttributeClauseTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<VirtualAttributeClause> VirtualAttributeClausePtr;

} // namespace common
} // namespace isearch

