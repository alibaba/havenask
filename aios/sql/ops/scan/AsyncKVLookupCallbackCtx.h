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
#include <string>
#include <utility>
#include <vector>

#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/util/AsyncCallbackCtxBase.h"

namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace indexlibv2 {
namespace config {
class ITabletSchema;
} // namespace config
namespace framework {
class ITablet;
} // namespace framework
} // namespace indexlibv2

namespace sql {

struct KVLookupOption {
    int64_t leftTime;
    autil::mem_pool::PoolPtr pool;
    size_t maxConcurrency;
    void *kvReader = nullptr;
    std::string indexName;
    std::string tableName;
    std::shared_ptr<indexlibv2::framework::ITablet> tablet = nullptr;
};

class AsyncKVLookupCallbackCtx : public AsyncCallbackCtxBase {
public:
    AsyncKVLookupCallbackCtx();
    ~AsyncKVLookupCallbackCtx() override;
    AsyncKVLookupCallbackCtx(const AsyncKVLookupCallbackCtx &) = delete;
    AsyncKVLookupCallbackCtx &operator=(const AsyncKVLookupCallbackCtx &) = delete;

public: // for kernel && scan
    // virtual for test
    virtual void start(std::vector<std::string> rawPks, KVLookupOption option);
    virtual bool
    isSchemaMatch(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) const = 0;
    const std::vector<autil::StringView *> &getResults() const;
    size_t getFailedCount() const;
    size_t getNotFoundCount() const;
    size_t getResultCount() const;
    std::string getLookupDesc() const;
    matchdoc::ReferenceBase *getRef() const;
    virtual bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const = 0;
    virtual int64_t getSeekTime() const = 0;

public: // for v1/v2
    virtual void asyncGet(KVLookupOption option) = 0;

private:
    void preparePksForSearch();

private:
    std::vector<std::string> _rawPks;

protected:
    std::vector<autil::StringView> _pksForSearch;
    std::vector<std::pair<autil::StringView, uint32_t>> _failedPks;
    std::vector<autil::StringView> _notFoundPks;
    std::vector<autil::StringView *> _results;
};

typedef std::shared_ptr<AsyncKVLookupCallbackCtx> AsyncKVLookupCallbackCtxPtr;

} // namespace sql
