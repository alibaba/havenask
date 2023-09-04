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
#include "suez/common/TargetRecorder.h"

#include <algorithm>
#include <cstddef>
#include <vector>

#include "alog/Logger.h"
#include "autil/FileRecorder.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace autil;
using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TargetRecorder);

namespace suez {

TargetRecorder::TargetRecorder()
    : _targetId(0), _finalTargetId(0), _currentId(0), _adminTargetId(0), _schedulerInfoId(0) {}

TargetRecorder::~TargetRecorder() {}

void TargetRecorder::newTarget(const string &target) {
    AUTIL_LOG(INFO, "target received");
    FileRecorder::newRecord(target, "heartbeat", "target", _targetId);
    _targetId++;
}

void TargetRecorder::newFinalTarget(const string &finalTarget) {
    AUTIL_LOG(INFO, "final target received");
    FileRecorder::newRecord(finalTarget, "heartbeat", "final", _finalTargetId);
    _finalTargetId++;
}

void TargetRecorder::newCurrent(const string &current) {
    FileRecorder::newRecord(current, "heartbeat", "current", _currentId);
    _currentId++;
}

void TargetRecorder::newSchedulerInfo(const string &info) {
    FileRecorder::newRecord(info, "heartbeat", "schedulerInfo", _schedulerInfoId);
    _schedulerInfoId++;
}

void TargetRecorder::newAdminTarget(const string &target) {
    FileRecorder::newRecord(target, "admin_target", "target", _adminTargetId);
    _adminTargetId++;
}

} // namespace suez
