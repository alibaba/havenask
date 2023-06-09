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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class ErrorResult;
class VirtualAttributeClause;
}  // namespace common
}  // namespace isearch
namespace suez {
namespace turing {
class SyntaxExpr;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace queryparser {

class VirtualAttributeParser
{
public:
    VirtualAttributeParser(common::ErrorResult *errorResult);
    ~VirtualAttributeParser();
private:
    VirtualAttributeParser(const VirtualAttributeParser &);
    VirtualAttributeParser& operator=(const VirtualAttributeParser &);
public:
    common::VirtualAttributeClause* createVirtualAttributeClause();
    void addVirtualAttribute(common::VirtualAttributeClause *virtualAttributeClause, 
                             const std::string &name, 
                             suez::turing::SyntaxExpr *syntaxExpr);
private:
    common::ErrorResult *_errorResult;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<VirtualAttributeParser> VirtualAttributeParserPtr;

} // namespace queryparser
} // namespace isearch

