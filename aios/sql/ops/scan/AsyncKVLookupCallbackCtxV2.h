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
#include <optional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Span.h"
#include "autil/result/Result.h"
#include "future_lite/CoroInterface.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtx.h"

namespace indexlibv2 {
namespace config {
class ITabletSchema;
} // namespace config
} // namespace indexlibv2
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace future_lite {
class Executor;
} // namespace future_lite

namespace navi {
class AsyncPipe;
} // namespace navi

namespace indexlibv2::framework {
class ITablet;
class ITabletReader;
} // namespace indexlibv2::framework

namespace sql {

struct AsyncKVLookupMetricsCollectorV2 {
    indexlibv2::index::KVMetricsCollector indexCollector;
    int64_t lookupTime = 0;
    size_t docsCount = 0;
    size_t failedDocsCount = 0;
    size_t notFoundDocsCount = 0;
};

class AsyncKVLookupCallbackCtxV2 : public AsyncKVLookupCallbackCtx,
                                   public std::enable_shared_from_this<AsyncKVLookupCallbackCtxV2> {
public:
    typedef indexlibv2::index::KVIndexReader::StatusPoolVector StatusVector;

public:
    AsyncKVLookupCallbackCtxV2(const std::shared_ptr<navi::AsyncPipe> &pipe,
                               future_lite::Executor *executor);
    virtual ~AsyncKVLookupCallbackCtxV2() = default;

public: // for callback
    void onSessionCallback(future_lite::interface::use_try_t<StatusVector> statusVecTry);

public:
    bool
    isSchemaMatch(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const override;
    void asyncGet(KVLookupOption option) override;
    bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const override;
    int64_t getSeekTime() const override;

private:
    void processStatusVec(future_lite::interface::use_try_t<StatusVector> statusVecTry);
    void prepareReadOptions(const KVLookupOption &option);
    void doWaitTablet();
    void onWaitTabletCallback(
        autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res);
    void doSearch(std::shared_ptr<indexlibv2::framework::ITablet> tablet);
    std::shared_ptr<indexlibv2::index::KVIndexReader>
    getReader(const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
              const std::string &indexName);
    void endLookupSession(std::optional<std::string> errorDesc);

private:
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;
    std::shared_ptr<indexlibv2::framework::ITabletReader> _tabletReader; // hold
    KVLookupOption _option;
    indexlibv2::index::KVReadOptions _readOptions;
    AsyncKVLookupMetricsCollectorV2 _metricsCollector;
    future_lite::Executor *_executor;
    std::vector<autil::StringView> _rawResults;
    std::optional<std::string> _errorDesc;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _schema;
};

} // namespace sql
