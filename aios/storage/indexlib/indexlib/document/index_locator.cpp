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
#include "indexlib/document/index_locator.h"

#include "autil/StringUtil.h"

namespace indexlib { namespace document {

const IndexLocator INVALID_INDEX_LOCATOR;

std::string IndexLocator::toString() const
{
    std::string result;
    result.resize(size());
    char* data = (char*)result.data();
    *(uint64_t*)data = _src;
    data += sizeof(_src);
    *(int64_t*)data = _offset;
    data += sizeof(_offset);
    _userData.copy(data, /*len=*/_userData.size());
    return result;
}

std::string IndexLocator::toDebugString() const
{
    std::string res = autil::StringUtil::toString(_src) + ":" + autil::StringUtil::toString(_offset);
    if (!_userData.empty()) {
        res = res + ":" + _userData;
    }
    return res;
}
}} // namespace indexlib::document
