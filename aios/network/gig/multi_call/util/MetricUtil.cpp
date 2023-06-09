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
#include "aios/network/gig/multi_call/util/MetricUtil.h"
#include <algorithm>

using namespace std;

namespace multi_call {

string MetricUtil::normalizeTag(const string &tag) {
    string tmp = tag; // copy on write
    ::replace(tmp.begin(), tmp.end(), '@', '-');
    return tmp;
}

string MetricUtil::normalizeEmpty(const std::string &tag) {
    if (!tag.empty()) {
        return tag;
    } else {
        return GIG_UNKNOWN;
    }
}

} // namespace multi_call
