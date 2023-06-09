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

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {

class EventTrackLog
{
public:
    EventTrackLog();
    EventTrackLog(const std::string &traceId, const std::string &traceContent);
    ~EventTrackLog();
    EventTrackLog(const EventTrackLog &) = delete;
    EventTrackLog& operator=(const EventTrackLog &) = delete;
  public:
    void setTraceId(const std::string &traceId) {
        _traceId = traceId;
    }
    void setTrackContent (const std::string &traceContent) {
        _traceContent = traceContent;
    }
  private:
      std::string _traceId;
      std::string _traceContent;
  private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<EventTrackLog> EventTrackLogPtr;

} //end namespace access_log
} //end namespace isearch

