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
#include "indexlib/index/attribute/AttrHelper.h"

#include "autil/ConstString.h"
#include "autil/MultiValueType.h"
#include "indexlib/index/common/FieldTypeTraits.h"

namespace indexlib::index {

std::vector<std::string> AttrHelper::ValidateAttrs(const std::vector<std::string>& attrs,
                                                   const std::vector<std::string>& inputAttrs)
{
    std::vector<std::string> validatedAttrs;
    for (auto attr : inputAttrs) {
        if (find(attrs.begin(), attrs.end(), attr) != attrs.end()) {
            validatedAttrs.push_back(attr);
        }
    }
    if (validatedAttrs.empty()) {
        validatedAttrs = attrs;
    }
    return validatedAttrs;
}

} // namespace indexlib::index
