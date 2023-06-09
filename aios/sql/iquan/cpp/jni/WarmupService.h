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

#include <vector>

#include "autil/AtomicCounter.h"
#include "autil/Log.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/config/WarmupConfig.h"
#include "iquan/jni/IquanDqlRequest.h"

namespace iquan {

class IquanImpl;

class WarmupService {
public:
    static Status warmup(IquanImpl *pIquan, const WarmupConfig &config);

private:
    static Status readJsonQuerys(const WarmupConfig &config, std::vector<IquanDqlRequest> &sqlQueryRequestList);
    static void warmupSingleThread(IquanImpl *pIquan,
                                   const WarmupConfig &config,
                                   int threadId,
                                   std::vector<IquanDqlRequest> &sqlQueryRequestList,
                                   autil::AtomicCounter &successCnt);
    static bool warmupQuery(IquanImpl *pIquan, IquanDqlRequest &request, int64_t &warmupNum, int64_t &failNum);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace iquan
