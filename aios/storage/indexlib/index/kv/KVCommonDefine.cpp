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
#include "indexlib/index/kv/KVCommonDefine.h"

#include "autil/Log.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.index, KVCommonDefine);

KVIndexType ParseKVIndexType(const std::string& str)
{
    if (str == "dense") {
        return KVIndexType::KIT_DENSE_HASH;
    } else if (str == "cuckoo") {
        return KVIndexType::KIT_CUCKOO_HASH;
    } else {
        AUTIL_LOG(WARN, "unknown hash type %s, use dense by default", str.c_str());
        return KVIndexType::KIT_DENSE_HASH;
    }
}

const char* PrintKVIndexType(KVIndexType type)
{
    switch (type) {
    case KVIndexType::KIT_DENSE_HASH:
        return "dense";
    case KVIndexType::KIT_CUCKOO_HASH:
        return "cuckoo";
    default:
        return "unknown";
    }
}

} // namespace indexlibv2::index
