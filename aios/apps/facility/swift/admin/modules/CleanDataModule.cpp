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
#include "swift/admin/modules/CleanDataModule.h"

#include <functional>
#include <stdint.h>

#include "autil/TimeUtility.h"
#include "swift/admin/CleanAtDeleteManager.h"
#include "swift/admin/SysController.h"
#include "swift/common/CreatorRegistry.h"
#include "swift/config/AdminConfig.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace protocol {
class TopicCreationRequest;
} // namespace protocol

namespace admin {

using namespace autil;
using namespace swift::protocol;

AUTIL_LOG_SETUP(swift, CleanDataModule);

CleanDataModule::CleanDataModule() {}

CleanDataModule::~CleanDataModule() {}

bool CleanDataModule::doInit() {
    _zkDataAccessor = _sysController->getAdminZkDataAccessor();
    return true;
}

bool CleanDataModule::doLoad() {
    _cleanAtDeleteManager = std::make_shared<CleanAtDeleteManager>();
    if (!_cleanAtDeleteManager->init(_adminConfig->getCleanAtDeleteTopicPatterns(),
                                     _zkDataAccessor,
                                     (uint32_t)(_adminConfig->getCleanDataIntervalHour() * 3600))) {
        AUTIL_LOG(ERROR, "load clean at delete topic tasks failed");
        return false;
    }
    _loopThread = LoopThread::createLoopThread(std::bind(&SysController::removeOldData, _sysController),
                                               int64_t(_adminConfig->getCleanDataIntervalHour() * 3600 * 1000 * 1000),
                                               "remove_data");
    if (!_loopThread) {
        AUTIL_LOG(ERROR, "create clean data thread fail");
        return false;
    }
    return true;
}

bool CleanDataModule::doUnload() {
    if (_loopThread) {
        _loopThread->stop();
    }
    return true;
}

bool CleanDataModule::isCleaningTopic(const std::string &topic) {
    if (_cleanAtDeleteManager) {
        return _cleanAtDeleteManager->isCleaningTopic(topic);
    }
    return false;
}

bool CleanDataModule::needCleanDataAtOnce(const std::string &topic) {
    if (_cleanAtDeleteManager) {
        return _cleanAtDeleteManager->needCleanDataAtOnce(topic);
    }
    return false;
}

void CleanDataModule::pushCleanAtDeleteTopic(const std::string &topic, const TopicCreationRequest &meta) {
    if (_cleanAtDeleteManager) {
        _cleanAtDeleteManager->pushCleanAtDeleteTopic(topic, meta);
    }
}

void CleanDataModule::removeCleanAtDeleteTopicData(const std::unordered_set<std::string> &loadedTopicSet) {
    if (_cleanAtDeleteManager) {
        _cleanAtDeleteManager->removeCleanAtDeleteTopicData(loadedTopicSet);
    }
}

REGISTER_MODULE(CleanDataModule, "M", "L");
} // namespace admin
} // namespace swift
