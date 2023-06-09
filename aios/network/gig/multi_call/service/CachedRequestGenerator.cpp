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
#include "aios/network/gig/multi_call/service/CachedRequestGenerator.h"

namespace multi_call {

void CachedRequestGenerator::generate(PartIdTy partCnt, PartRequestMap &requestMap) {
    auto iter = _cachedPartRequestMap.find(partCnt);
    if (iter == _cachedPartRequestMap.end()) {
        _generator->generate(partCnt, requestMap);
        if (!_clearCacheAfterGet) {
            _cachedPartRequestMap.emplace(partCnt, requestMap);
        }
    } else {
        if (!_clearCacheAfterGet) {
            requestMap = iter->second;
        } else {
            requestMap.swap(iter->second);
            _cachedPartRequestMap.erase(iter);
        }
    }
}

std::string CachedRequestGenerator::toString() const {
    std::stringstream stream;
    for (auto &iter : _cachedPartRequestMap) {
        stream << "p:" << iter.first << ",r:" << iter.second.size() << ";";
    }
    return stream.str();
}

} // namespace multi_call