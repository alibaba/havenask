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
#include "suez/common/TimeRecorder.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TimeRecorder);

namespace suez {

TimeRecorder::TimeRecorder() { _beginTimeStamp = autil::TimeUtility::currentTime(); }

TimeRecorder::~TimeRecorder() {}

void TimeRecorder::record(const string &msg) {
    int64_t currentTime = autil::TimeUtility::currentTime();
    double usedTime = (double)(currentTime - _beginTimeStamp) / 1000.0;
    AUTIL_LOG(DEBUG, "%s used time %f", msg.c_str(), usedTime);
}

void TimeRecorder::recordAndReset(const string &msg) {
    record(msg);
    reset();
}

void TimeRecorder::reset() { _beginTimeStamp = autil::TimeUtility::currentTime(); }

} // namespace suez
