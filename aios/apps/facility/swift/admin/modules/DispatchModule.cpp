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
#include "swift/admin/modules/DispatchModule.h"

#include <functional>
#include <stdint.h>

#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"
#include "swift/monitor/AdminMetricsReporter.h"

namespace swift {
namespace admin {

using namespace autil;

AUTIL_LOG_SETUP(swift, DispatchModule);

DispatchModule::DispatchModule() : _reporter(nullptr) {}

DispatchModule::~DispatchModule() {}

bool DispatchModule::doInit() {
    _reporter = _sysController->getMetricsReporter();
    return true;
}

bool DispatchModule::doLoad() {
    _loopThread = LoopThread::createLoopThread(
        std::bind(&DispatchModule::workLoop, this), _adminConfig->getDispatchIntervalMs() * 1000, "dispatch");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create dispatch thread fail");
        return false;
    }
    return true;
}

bool DispatchModule::doUnload() {
    if (_loopThread) {
        _loopThread->stop();
    }
    return true;
}

void DispatchModule::workLoop() {
    int64_t begTime = TimeUtility::currentTime();
    _sysController->doExecute();
    int64_t endTime = TimeUtility::currentTime();
    if (_reporter) {
        _reporter->reportScheduleTime(endTime - begTime);
    }
    if (endTime - begTime > 500 * 1000) { // 500ms
        AUTIL_LOG(INFO, "schedule too slow, used[%ldms]", (endTime - begTime) / 1000);
    }
}

REGISTER_MODULE(DispatchModule, "M|S", "L");
} // namespace admin
} // namespace swift
