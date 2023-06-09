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

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/index/kv/Types.h"

namespace autil {
class TimeoutTerminator;
}

namespace indexlibv2::index {

struct KVReadOptions {
    uint64_t timestamp = 0; // unit : microseconds, you can set it to current time stamp in ordinary case
    std::shared_ptr<autil::TimeoutTerminator> timeoutTerminator;
    indexlib::TableSearchCacheType searchCacheType = indexlib::tsc_default;
    autil::mem_pool::Pool* pool = NULL; // you should set it not null in ordinary case
    std::string fieldName;              // valid when multi field
    size_t maxConcurrency =
        0;              // concurrency of batch get. default value of 0 means concurrecy size equals to keys count
    bool yield = false; // should yield between two batches
    attrid_t attrId = INVALID_ATTRID;
    index::KVMetricsCollector* metricsCollector = NULL;
};

} // namespace indexlibv2::index
