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
#pragma once

#include <cstdint>
#include <limits>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/jsonizable.h"
#include "ha3/util/TypeDefine.h"

namespace isearch {
namespace config {

class SearcherCacheConfig : public autil::legacy::Jsonizable {
public:
    SearcherCacheConfig() {
        maxItemNum = 200000;
        maxSize = 0; //MB
        incDocLimit = std::numeric_limits<uint32_t>::max();
        incDeletionPercent = 100;
        latencyLimitInMs = 0.0; // ms
        minAllowedCacheDocNum = 0;
        maxAllowedCacheDocNum = std::numeric_limits<uint32_t>::max();
    }
    ~SearcherCacheConfig() {};
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
        JSONIZE(json, "max_item_num", maxItemNum);
        JSONIZE(json, "max_size", maxSize);
        JSONIZE(json, "inc_doc_limit", incDocLimit);
        JSONIZE(json, "inc_deletion_percent", incDeletionPercent);
        JSONIZE(json, "latency_limit", latencyLimitInMs);
        JSONIZE(json, "min_allowed_cache_doc_num", minAllowedCacheDocNum);
        JSONIZE(json, "max_allowed_cache_doc_num", maxAllowedCacheDocNum);
    }
public:
    uint32_t maxItemNum;
    uint32_t maxSize;
    uint32_t incDocLimit;
    uint32_t incDeletionPercent;
    float latencyLimitInMs;
    uint32_t minAllowedCacheDocNum;
    uint32_t maxAllowedCacheDocNum;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherCacheConfig> SearcherCacheConfigPtr;

} // namespace config
} // namespace isearch
