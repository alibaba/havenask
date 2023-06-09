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
#include "ha3/common/VirtualAttributeClause.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, VirtualAttributeClause);

VirtualAttributeClause::VirtualAttributeClause() { 
}

VirtualAttributeClause::~VirtualAttributeClause() { 
    for (vector<VirtualAttribute*>::iterator it = _virtualAttributes.begin(); 
         it != _virtualAttributes.end(); ++it) 
    {
        delete (*it);
    }
    _virtualAttributes.clear();
}

void VirtualAttributeClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_virtualAttributes);
}

void VirtualAttributeClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_virtualAttributes);
}

bool VirtualAttributeClause::addVirtualAttribute(const std::string &name, 
                           SyntaxExpr *syntaxExpr) 
{
    if (name.empty() || NULL == syntaxExpr) {
        AUTIL_LOG(WARN, "add virtualAttribute failed, name[%s] is empty or"
                " syntaxExpr is NULL", name.c_str());
        return false;
    }
    for (vector<VirtualAttribute*>::const_iterator it = _virtualAttributes.begin(); 
         it != _virtualAttributes.end(); ++it) 
    {
        if ((*it)->getVirtualAttributeName() == name) {
            AUTIL_LOG(WARN, "add virtualAttribute failed, same name[%s] virtualAttribute "
                    "already existed", name.c_str());
            return false;
        }
    }
    
    _virtualAttributes.push_back(new VirtualAttribute(name, syntaxExpr));
    return true;
}

string VirtualAttributeClause::toString() const {
    string vaClauseStr;
    size_t vaCount = _virtualAttributes.size();
    for (size_t i = 0; i < vaCount; i++) {
        VirtualAttribute* virtualAttribute = _virtualAttributes[i];
        vaClauseStr.append("[");
        vaClauseStr.append(virtualAttribute->getVirtualAttributeName());
        vaClauseStr.append(":");
        vaClauseStr.append(virtualAttribute->getExprString());
        vaClauseStr.append("]");
    }
    return vaClauseStr;
}

} // namespace common
} // namespace isearch

