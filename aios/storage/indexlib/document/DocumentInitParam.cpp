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
#include "indexlib/document/DocumentInitParam.h"

#include <utility>

namespace indexlibv2::document {

void DocumentInitParam::AddValue(const std::string& key, const std::string& value) { _kvMap[key] = value; }

bool DocumentInitParam::GetValue(const std::string& key, std::string& value)
{
    KeyValueMap::const_iterator iter = _kvMap.find(key);
    if (iter == _kvMap.end()) {
        return false;
    }
    value = iter->second;
    return true;
}

} // namespace indexlibv2::document
