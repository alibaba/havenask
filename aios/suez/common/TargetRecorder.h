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

#include <stddef.h>
#include <string>

namespace suez {

class TargetRecorder {
public:
    TargetRecorder();
    ~TargetRecorder();

public:
    void newTarget(const std::string &target);
    void newFinalTarget(const std::string &finalTarget);
    void newCurrent(const std::string &current);
    void newAdminTarget(const std::string &target);
    void newSchedulerInfo(const std::string &info);

private:
    size_t _targetId;
    size_t _finalTargetId;
    size_t _currentId;
    size_t _adminTargetId;
    size_t _schedulerInfoId;
};

} // namespace suez
