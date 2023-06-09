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
#include <set>
#include <stdint.h>
#include <string>

#include "autil/StringUtil.h"
#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/common/WatermarkType.h"
#include "ha3/sql/ops/util/AsyncCallbackCtxBase.h"
#include "indexlib/index/kv/KVCommonDefine.h"

namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc
namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor

namespace isearch {
namespace sql {

class TabletManagerR;

struct KVLookupOption {
    int64_t leftTime;
    autil::mem_pool::PoolPtr pool;
    size_t maxConcurrency;
    void *kvReader = nullptr;
    int64_t targetWatermark = 0;
    WatermarkType targetWatermarkType = WatermarkType::WM_TYPE_DISABLED;
    double targetWatermarkTimeoutRatio = 0.5;
    std::string indexName;
    std::string tableName;
    TabletManagerR *tabletManagerR = nullptr;
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
    const std::vector<autil::StringView *> &getResults() const;
    size_t getFailedCount() const;
    size_t getNotFoundCount() const;
    size_t getResultCount() const;
    std::string getLookupDesc() const;
    matchdoc::ReferenceBase *getRef() const;
    virtual bool tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const = 0;
    virtual size_t getDegradeDocsSize() const = 0;
    virtual int64_t getSeekTime() const = 0;
    virtual int64_t getWaitWatermarkTime() const {
        return 0;
    }
    virtual int64_t getBuildWatermark() const {
        return 0;
    }

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

} // end namespace sql
} // end namespace isearch
