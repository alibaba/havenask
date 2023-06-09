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
#ifndef ISEARCH_BS_BUILDSPEEDLIMITER_H
#define ISEARCH_BS_BUILDSPEEDLIMITER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace builder {

class BuildSpeedLimiter
{
private:
    static const int64_t ONE_SECOND = 1LL * 1000 * 1000;

public:
    BuildSpeedLimiter(const std::string& buildIdStr = "");
    ~BuildSpeedLimiter();

private:
    BuildSpeedLimiter(const BuildSpeedLimiter&);
    BuildSpeedLimiter& operator=(const BuildSpeedLimiter&);

public:
    void setTargetQps(int32_t sleepPerDoc, uint32_t buildQps);
    void enableLimiter() { _enableLimiter = true; }
    int32_t getTargetQps() const { return _targetQps; };
    void limitSpeed();

private:
    std::string _buildIdStr;
    uint32_t _targetQps;
    uint32_t _counter;
    int64_t _lastCheckTime;
    int32_t _sleepPerDoc;
    bool _enableLimiter; // enable only when realtime

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildSpeedLimiter);

}} // namespace build_service::builder

#endif // ISEARCH_BS_BUILDSPEEDLIMITER_H
