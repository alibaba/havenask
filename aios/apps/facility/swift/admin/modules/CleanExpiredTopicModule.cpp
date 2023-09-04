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
#include "swift/admin/modules/CleanExpiredTopicModule.h"

#include <functional>
#include <memory>
#include <stdint.h>

#include "autil/TimeUtility.h"
#include "swift/admin/SysController.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"

namespace swift {
namespace admin {

using namespace autil;

AUTIL_LOG_SETUP(swift, CleanExpiredTopicModule);

CleanExpiredTopicModule::CleanExpiredTopicModule() {}

CleanExpiredTopicModule::~CleanExpiredTopicModule() {}

bool CleanExpiredTopicModule::doLoad() {
    _loopThread = LoopThread::createLoopThread(std::bind(&SysController::deleteExpiredTopic, _sysController),
                                               int64_t(_adminConfig->getDelExpiredTopicIntervalSec() * 1000 * 1000),
                                               "delete_topic");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create clean expired topic thread fail");
        return false;
    }
    return true;
}

bool CleanExpiredTopicModule::doUnload() {
    if (_loopThread) {
        _loopThread->stop();
    }
    return true;
}

REGISTER_MODULE(CleanExpiredTopicModule, "M", "L");

} // namespace admin
} // namespace swift
