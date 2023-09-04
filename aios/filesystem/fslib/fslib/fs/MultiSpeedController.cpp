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
#include "fslib/fs/MultiSpeedController.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"

using namespace std;

FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, MultiSpeedController);

MultiSpeedController::MultiSpeedController() {
    string paramStr = autil::EnvUtil::getEnv("FSLIB_READ_SPEED_LIMIT");
    init(paramStr);
}

MultiSpeedController::~MultiSpeedController() {}

void MultiSpeedController::init(const std::string &speedLimitParam) {
    vector<string> patternStrVec;
    autil::StringUtil::fromString(speedLimitParam, patternStrVec, ";");
    for (auto &pattern : patternStrVec) {
        SpeedControllerPtr controller(new SpeedController());
        controller->initFromString(pattern);
        _readSpeedControllers.push_back(move(controller));
    }
    SpeedControllerPtr controller(new SpeedController());
    controller->initFromString("");
    _readSpeedControllers.push_back(move(controller));
}

SpeedController *MultiSpeedController::getReadSpeedController(const std::string &filePath) {
    MultiSpeedController *multiSpeedController = MultiSpeedController::getInstance();
    return multiSpeedController->findReadSpeedController(filePath);
}

void MultiSpeedController::getQuotaDetailInfo(std::vector<std::pair<std::string, int64_t>> &quotaInfos) {
    MultiSpeedController *multiSpeedController = MultiSpeedController::getInstance();
    for (const auto &controller : multiSpeedController->_readSpeedControllers) {
        const std::string &pathPattern = controller->getPathPattern();
        if (pathPattern.empty()) {
            continue;
        }
        quotaInfos.push_back(std::make_pair(pathPattern, controller->getQuotaPerSecond()));
    }
}

bool MultiSpeedController::updateQuota(const std::string &patternStr, int64_t quotaSizePerSec) {
    MultiSpeedController *multiSpeedController = MultiSpeedController::getInstance();
    SpeedController *controller = multiSpeedController->findReadSpeedControllerByPatternString(patternStr);
    if (!controller) {
        return false;
    }
    controller->resetQuotaPerSecond(quotaSizePerSec);
    return true;
}

SpeedController *MultiSpeedController::findReadSpeedController(const std::string &filePath) const {
    for (const auto &controller : _readSpeedControllers) {
        if (controller->matchPathPattern(filePath)) {
            return controller.get();
        }
    }
    assert(false);
    return NULL;
}

SpeedController *MultiSpeedController::findReadSpeedControllerByPatternString(const std::string &pattern) const {
    for (const auto &controller : _readSpeedControllers) {
        if (pattern == controller->getPathPattern()) {
            return controller.get();
        }
    }
    return NULL;
}

double MultiSpeedController::getQuotaUsageRatio(const std::string &patternString) {
    MultiSpeedController *multiSpeedController = MultiSpeedController::getInstance();
    auto controller = multiSpeedController->findReadSpeedControllerByPatternString(patternString);
    return controller != nullptr ? controller->getQuotaUsageRatio() : 1.0;
}

FSLIB_END_NAMESPACE(fs);
