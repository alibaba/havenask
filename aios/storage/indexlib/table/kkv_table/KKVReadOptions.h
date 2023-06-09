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

namespace autil {
class TimeoutTerminator;
}

namespace indexlibv2::index {
class KVMetricsCollector;
}
namespace indexlibv2::table {

// copy from KVReadOptions
struct KKVReadOptions {
    // unit : microseconds, you can set it to current timestamp in ordinary case
    uint64_t timestamp = 0;
    std::shared_ptr<autil::TimeoutTerminator> timeoutTerminator;
    indexlib::TableSearchCacheType searchCacheType = indexlib::tsc_default;
    // you should set it not null in ordinary case
    autil::mem_pool::Pool* pool = nullptr;
    // valid when multi field
    std::string fieldName;
    // concurrency of batch get. default value of 0 means concurrecy size equals to keys count
    size_t maxConcurrency = 0;
    // should yield between two batches
    bool yield = false;
    attrid_t attrId = INVALID_ATTRID;
    index::KVMetricsCollector* metricsCollector = nullptr;
};

} // namespace indexlibv2::table
