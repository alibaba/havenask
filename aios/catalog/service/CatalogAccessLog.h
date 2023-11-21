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

#include <iosfwd>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"

namespace catalog {

template <typename Treq, typename Tres>
class CatalogAccessLog {
public:
    // lifetime of request and response should be longer than this instance
    CatalogAccessLog(const Treq *request, const Tres *response, const std::string funcName)
        : _request(request), _response(response), _funcName(funcName) {
        _startTime = autil::TimeUtility::currentTime();
    }
    ~CatalogAccessLog() { log(); }

private:
    CatalogAccessLog(const CatalogAccessLog &);
    CatalogAccessLog &operator=(const CatalogAccessLog &);

private:
    void log() {
        int64_t endTime = autil::TimeUtility::currentTime();
        AUTIL_LOG(INFO,
                  "func=[%s], response=[%s], request=[%s], cost=[%dms]",
                  _funcName.c_str(),
                  (_response ? _response->ShortDebugString().c_str() : "null"),
                  (_request ? _request->ShortDebugString().c_str() : "null"),
                  int32_t((endTime - _startTime) / 1000));
    }

private:
    int64_t _startTime;
    const Treq *_request;
    const Tres *_response;
    const std::string _funcName;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(catalog, CatalogAccessLog, Treq, Tres);

} // namespace catalog
