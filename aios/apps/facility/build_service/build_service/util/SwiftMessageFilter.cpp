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
#include "build_service/util/SwiftMessageFilter.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "build_service/util/LocatorUtil.h"

using namespace std;

namespace build_service::util {
BS_LOG_SETUP(util, SwiftMessageFilter);

SwiftMessageFilter::SwiftMessageFilter() {}

SwiftMessageFilter::~SwiftMessageFilter() {}
void SwiftMessageFilter::setSeekProgress(const indexlibv2::base::ProgressVector& progress)
{
    BS_LOG(INFO, "swift message filter seek progress [%s]", LocatorUtil::progress2DebugString(progress).c_str());
    _seekProgress = progress;
}

bool SwiftMessageFilter::filterOrRewriteProgress(uint16_t payload, indexlibv2::base::Progress::Offset offset,
                                                 indexlibv2::base::ProgressVector* progress)
{
    if (_seekProgress.empty()) {
        return false;
    }
    for (auto& progress : _seekProgress) {
        if (progress.from <= payload && payload <= progress.to) {
            if (offset < progress.offset) {
                _filteredMessageCount++;
                return true;
            }
        }
    }
    _seekProgress = LocatorUtil::computeProgress(_seekProgress, *progress, LocatorUtil::maxOffset);
    if (_seekProgress == *progress) {
        BS_LOG(INFO, "no longer need filter message, total filtered doc count [%ld], current progress[%s]",
               _filteredMessageCount, LocatorUtil::progress2DebugString(*progress).c_str());
        _seekProgress.clear();
    } else {
        *progress = _seekProgress;
    }
    return false;
}

void SwiftMessageFilter::setSeekProgress(const indexlibv2::base::MultiProgress& multiProgress)
{
    BS_LOG(INFO, "swift message filter seek multi progress [%s]",
           LocatorUtil::progress2DebugString(multiProgress).c_str());
    _seekMultiProgress = multiProgress;
}

bool SwiftMessageFilter::needSkip(const indexlibv2::framework::Locator::DocInfo& docInfo)
{
    if (_seekMultiProgress.empty()) {
        return false;
    }
    assert(docInfo.sourceIdx < _seekMultiProgress.size());
    if (_seekMultiProgress[docInfo.sourceIdx].empty()) {
        return false;
    }
    for (auto& singleProgress : _seekMultiProgress[docInfo.sourceIdx]) {
        if (singleProgress.from <= docInfo.hashId && docInfo.hashId <= singleProgress.to) {
            if (docInfo.GetProgressOffset() < singleProgress.offset) {
                _filteredMessageCount++;
                return true;
            }
        }
    }
    return false;
}

bool SwiftMessageFilter::rewriteProgress(indexlibv2::base::MultiProgress* progress)
{
    if (_seekMultiProgress.empty()) {
        return true;
    }
    auto ret = LocatorUtil::computeMultiProgress(_seekMultiProgress, *progress, LocatorUtil::maxOffset);
    if (ret.has_value()) {
        _seekMultiProgress = ret.value();
    } else {
        BS_LOG(ERROR, "compute max mulit progress failed, seek progress [%s], doc progress [%s]",
               LocatorUtil::progress2DebugString(_seekMultiProgress).c_str(),
               LocatorUtil::progress2DebugString(*progress).c_str());
        return false;
    }
    if (_seekMultiProgress == *progress) {
        BS_LOG(INFO, "no longer need filter message, total filtered doc count [%ld], current progress[%s]",
               _filteredMessageCount, LocatorUtil::progress2DebugString(*progress).c_str());
        _seekMultiProgress.clear();
    } else {
        *progress = _seekMultiProgress;
    }
    return true;
}

} // namespace build_service::util
