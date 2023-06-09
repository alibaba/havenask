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
#include "ha3/config/ResourceTypeSet.h"

#include <cstddef>

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, ResourceTypeSet);

ResourceTypeSet::ResourceTypeSet() { 
}

ResourceTypeSet::~ResourceTypeSet() { 
}

void ResourceTypeSet::fromString(const string &str) {
    _types.clear();
    StringTokenizer tokens(str, ",", StringTokenizer::TOKEN_IGNORE_EMPTY | 
                           StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < tokens.getNumTokens(); i++) {
        _types.insert(tokens[i]);
    }
}
    
string ResourceTypeSet::toString() const {
    string str;
    for (set<string>::const_iterator it = _types.begin(); 
         it != _types.end(); it++)
    {
        if (str.empty()) {
            str = *it;
        } else {
            str += "," + *it;
        }
    }
    return str;
}

bool ResourceTypeSet::empty() const {
    return _types.empty();
}


} // namespace config
} // namespace isearch

