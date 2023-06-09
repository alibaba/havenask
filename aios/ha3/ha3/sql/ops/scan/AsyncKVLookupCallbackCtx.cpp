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
#include "ha3/sql/ops/scan/AsyncKVLookupCallbackCtx.h"

#include <assert.h>
#include <iosfwd>
#include <type_traits>
#include <utility>

#include "autil/TimeUtility.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Executor.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/kv_reader.h"
#include "matchdoc/Reference.h"
#include "navi_ops/coroutine/NaviAsyncPipeExecutor.h"

using namespace std;
using namespace autil;
using namespace navi;
using namespace indexlibv2::index;
using namespace future_lite::interface;

namespace isearch {
namespace sql {

AsyncKVLookupCallbackCtx::AsyncKVLookupCallbackCtx() {}

AsyncKVLookupCallbackCtx::~AsyncKVLookupCallbackCtx() {}

void AsyncKVLookupCallbackCtx::start(std::vector<std::string> rawPks,
                                     KVLookupOption option) {
    NAVI_LOG(TRACE3,
             "start lookup with rawPks[%s], leftTime[%ld], maxConcurrency[%ld] "
             "targetWatermark[%ld] targetWatermarkType[%d]",
             autil::StringUtil::toString(rawPks).c_str(),
             option.leftTime,
             option.maxConcurrency,
             option.targetWatermark,
             option.targetWatermarkType);
    incStartVersion();
    _rawPks = std::move(rawPks);
    preparePksForSearch();
    asyncGet(std::move(option));
}

void AsyncKVLookupCallbackCtx::preparePksForSearch() {
    _pksForSearch.clear();
    _pksForSearch.reserve(_rawPks.size());
    for (const auto &rawPk : _rawPks) {
        _pksForSearch.emplace_back(rawPk.c_str(), rawPk.size());
    }
    _failedPks.clear();
    _notFoundPks.clear();
}

std::string AsyncKVLookupCallbackCtx::getLookupDesc() const {
    assert(!isInFlightNoLock());
    std::stringstream ss;
    ss << "lookup done. total[" << _pksForSearch.size() << "] failedPks [";
    autil::StringUtil::toStream(ss, _failedPks);
    ss << "] notFoundPks [";
    autil::StringUtil::toStream(ss, _notFoundPks);
    ss << "]";
    return ss.str();
}

const std::vector<autil::StringView *> &AsyncKVLookupCallbackCtx::getResults() const {
    assert(!isInFlightNoLock());
    assert(_results.size() == _pksForSearch.size());
    return _results;
}

size_t AsyncKVLookupCallbackCtx::getFailedCount() const {
    return _failedPks.size();
}

size_t AsyncKVLookupCallbackCtx::getNotFoundCount() const {
    return _notFoundPks.size();
}

size_t AsyncKVLookupCallbackCtx::getResultCount() const {
    return _results.size();
}

} // end namespace sql
} // end namespace isearch
