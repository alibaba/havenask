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
#include "swift/admin/modules/ChangeTopicPartModule.h"

#include <functional>
#include <memory>

#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"

namespace swift {
namespace admin {

using namespace autil;

AUTIL_LOG_SETUP(swift, ChangeTopicPartModule);

ChangeTopicPartModule::ChangeTopicPartModule() {}
ChangeTopicPartModule::~ChangeTopicPartModule() {}

bool ChangeTopicPartModule::doLoad() {
    _loopThread = LoopThread::createLoopThread(std::bind(&SysController::changeTopicPartCnt, _sysController),
                                               _adminConfig->getChgPartcntIntervalUs(),
                                               "chgPrtCnt");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create change topic partcnt thread fail");
        return false;
    } else {
        AUTIL_LOG(INFO, "create change topic partcnt thread success, interval 2s");
        return true;
    }
}

bool ChangeTopicPartModule::doUnload() {
    if (_loopThread) {
        _loopThread->stop();
    }
    return true;
}

REGISTER_MODULE(ChangeTopicPartModule, "M", "L");

} // namespace admin
} // namespace swift
