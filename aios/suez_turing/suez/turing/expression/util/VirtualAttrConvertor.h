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

#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class SyntaxExpr;

class VirtualAttrConvertor {
public:
    VirtualAttrConvertor();
    ~VirtualAttrConvertor();

private:
    VirtualAttrConvertor(const VirtualAttrConvertor &);
    VirtualAttrConvertor &operator=(const VirtualAttrConvertor &);

public:
    void initVirtualAttrs(const VirtualAttributes &virtualAttributes);
    std::string nameToName(const std::string &attrName) const;
    const SyntaxExpr *exprToExpr(const SyntaxExpr *syntax) const;
    const SyntaxExpr *nameToExpr(const std::string &exprName) const;

private:
    VirtualAttrMap _virtualAttrs;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
