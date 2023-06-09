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
#include "autil/NumberHashFunction.h"

#include <iosfwd>

#include "autil/StringUtil.h"

using namespace std;

namespace autil {

int64_t NumberHashFunction::hashString(const string& str) const
{
    int64_t num;
    if (StringUtil::fromString(str, num)) {
        return num;
    } else {
        return 0;
    }
}

uint32_t NumberHashFunction::getHashId(const std::string& str) const
{
    if (_hashSize == 0) {
        return 0;
    }
    return (uint32_t)((uint64_t)this->hashString(str) % _hashSize);
}

}

