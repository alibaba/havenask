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
#include "aios/network/gig/multi_call/heartbeat/MetaUtil.h"
#include <map>
#include <set>
#include <string>

namespace multi_call {

int MetaUtil::addExtendTo(const BizMeta &from, BizMeta *to, bool is_override) {
    assert(to != nullptr);
    std::set<std::string> current_keys;
    if (!is_override) {
        for (auto &metaKV : to->extend()) {
            current_keys.insert(metaKV.key());
        }
    }

    size_t newMetaKVCount = 0;
    for (auto &metaKV : from.extend()) {
        if (is_override ||
            current_keys.find(metaKV.key()) == current_keys.end()) {
            *to->add_extend() = metaKV;
            ++newMetaKVCount;
        }
    }
    return newMetaKVCount;
}

} // namespace multi_call
