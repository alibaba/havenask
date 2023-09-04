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

#include "build_service/util/LocatorUtil.h"

using namespace std;

namespace build_service::util {
BS_LOG_SETUP(util, SwiftMessageFilter);

SwiftMessageFilter::SwiftMessageFilter() {}

SwiftMessageFilter::~SwiftMessageFilter() {}
void SwiftMessageFilter::setSeekProgress(const std::vector<indexlibv2::base::Progress>& progress)
{
    BS_LOG(INFO, "swift message filter seek progress [%s]", LocatorUtil::progress2DebugString(progress).c_str());
    _seekProgress = progress;
}

bool SwiftMessageFilter::filterOrRewriteProgress(uint16_t payload, indexlibv2::base::Progress::Offset offset,
                                                 std::vector<indexlibv2::base::Progress>* progress)
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
    _seekProgress = LocatorUtil::ComputeProgress(_seekProgress, *progress, LocatorUtil::maxOffset);
    if (_seekProgress == *progress) {
        BS_LOG(INFO, "no longer need filter message, total filtered doc count [%ld], current progress[%s]",
               _filteredMessageCount, LocatorUtil::progress2DebugString(*progress).c_str());
        _seekProgress.clear();
    } else {
        *progress = _seekProgress;
    }
    return false;
}

} // namespace build_service::util
