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

#include <stdint.h>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Progress.h"
#include "indexlib/framework/Locator.h"

namespace build_service::util {

class SwiftMessageFilter
{
public:
    SwiftMessageFilter();
    ~SwiftMessageFilter();

public:
    void setSeekProgress(const indexlibv2::base::MultiProgress& progress);

    bool needSkip(const indexlibv2::framework::Locator::DocInfo& docInfo);

    void setSeekProgress(const indexlibv2::base::ProgressVector& progress);
    // 返回true时，代表需要过滤。否则，改写progress
    bool filterOrRewriteProgress(uint16_t payload, indexlibv2::base::Progress::Offset offset,
                                 indexlibv2::base::ProgressVector* progress);
    bool rewriteProgress(indexlibv2::base::MultiProgress* progress);

private:
    indexlibv2::base::ProgressVector _seekProgress;
    indexlibv2::base::MultiProgress _seekMultiProgress;
    int64_t _filteredMessageCount = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftMessageFilter);

} // namespace build_service::util
