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
#include "matchdoc/ReferenceSet.h"

namespace matchdoc {

ReferenceSet::~ReferenceSet() {
    for (auto ref : referenceVec) {
        delete ref;
    }
}

bool ReferenceSet::rename(const std::string &src, const std::string &dst) {
    auto it = referenceMap.find(src);
    if (it == referenceMap.end()) {
        return false;
    }
    if (src == dst) {
        return true;
    }
    if (referenceMap.count(dst) > 0) {
        return false;
    }
    ReferenceBase *ref = it->second;
    ref->setName(dst);
    referenceMap.erase(it);
    referenceMap.emplace(dst, ref);
    return true;
}

bool ReferenceSet::remove(const std::string &name) {
    auto it = referenceMap.find(name);
    if (it != referenceMap.end()) {
        const auto &name = it->first;
        auto vit = std::remove_if(referenceVec.begin(), referenceVec.end(), [&name](const ReferenceBase *ref) {
            return name == ref->getName();
        });
        referenceVec.erase(vit);
        delete it->second;
        referenceMap.erase(it);
    }
    return referenceMap.empty();
}

bool ReferenceSet::equals(const ReferenceSet &other) const {
    if (referenceMap.size() != other.referenceMap.size()) {
        return false;
    }
    for (auto it1 = referenceMap.begin(), it2 = other.referenceMap.begin();
         it1 != referenceMap.end() && it2 != other.referenceMap.end();
         ++it1, ++it2) {
        if (it1->first != it2->first) {
            return false;
        }
        if (!it1->second->equals(*(it2->second))) {
            return false;
        }
    }
    return true;
}

void ReferenceSet::sort() {
    std::sort(referenceVec.begin(), referenceVec.end(), [](auto *l, auto *r) { return l->getName() < r->getName(); });
}

} // namespace matchdoc
