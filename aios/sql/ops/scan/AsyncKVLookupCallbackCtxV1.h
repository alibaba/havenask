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
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/Span.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/index/kv/kv_read_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtx.h"

namespace future_lite {
class Executor;
template <typename T>
class Try;
} // namespace future_lite
namespace indexlibv2 {
namespace config {
class ITabletSchema;
} // namespace config
} // namespace indexlibv2
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace indexlib {
namespace index {
typedef std::shared_ptr<KVReader> KVReaderPtr;
} // namespace index
} // namespace indexlib

namespace navi {
class AsyncPipe;
} // namespace navi

namespace sql {

struct AsyncKVLookupMetricsCollectorV1 {
    indexlib::index::KVMetricsCollector indexCollector;
    int64_t lookupTime = 0;
    size_t docsCount = 0;
    size_t failedDocsCount = 0;
    size_t notFoundDocsCount = 0;
};

class AsyncKVLookupCallbackCtxV1 : public AsyncKVLookupCallbackCtx,
                                   public std::enable_shared_from_this<AsyncKVLookupCallbackCtxV1> {
public:
    typedef indexlib::index::KVReader::BoolPoolVector BoolVector;

public:
    AsyncKVLookupCallbackCtxV1(const std::shared_ptr<navi::AsyncPipe> &pipe,
                               future_lite::Executor *executor);

    virtual ~AsyncKVLookupCallbackCtxV1() = default;

public: // for callback
    void onSessionCallback(future_lite::Try<BoolVector> &boolVecTry);

public:
    bool
    isSchemaMatch(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const override {
        return true;
    }
    bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const override;
    void asyncGet(KVLookupOption option) override;
    size_t getDegradeDocsSize() const override;
    int64_t getSeekTime() const override;

private:
    void processBoolVec(BoolVector &boolVec);
    void prepareReadOptions(const KVLookupOption &option);

private:
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    autil::mem_pool::PoolPtr _pool;
    indexlib::index::KVReadOptions _readOptions;
    std::vector<autil::StringView> _rawResults;
    AsyncKVLookupMetricsCollectorV1 _metricsCollector;
    future_lite::Executor *_executor;
};

} // namespace sql
