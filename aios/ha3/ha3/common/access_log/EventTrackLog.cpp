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
#include "ha3/common/access_log/EventTrackLog.h"

using namespace std;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, EventTrackLog);

static std::string EVENT_TRACK_SPLIT = " ## ";
static std::string EVENT_TRACK_KV_SPLIT = ":";

EventTrackLog::EventTrackLog() { 
}

EventTrackLog::EventTrackLog(const std::string &traceId, const std::string &traceContent)
    : _traceId(traceId)
    , _traceContent(traceContent) {}

EventTrackLog::~EventTrackLog() {
    AUTIL_LOG(INFO,
              "trace_id:%s ## trace_content:%s",
              _traceId.c_str(),
              _traceContent.c_str());
}

} //end namespace access_log
} //end namespace isearch

