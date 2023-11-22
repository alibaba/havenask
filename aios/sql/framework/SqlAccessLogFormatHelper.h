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

#include "multi_call/stream/GigStreamRpcInfo.h"

namespace sql {

class SqlAccessLog;

class SqlAccessLogFormatHelper {
public:
    SqlAccessLogFormatHelper(const SqlAccessLog &accessLog);
    ~SqlAccessLogFormatHelper();
    SqlAccessLogFormatHelper(const SqlAccessLogFormatHelper &) = delete;
    SqlAccessLogFormatHelper &operator=(const SqlAccessLogFormatHelper &) = delete;

public:
    const SqlAccessLog *getAccessLog() const {
        return &_accessLog;
    }
    bool hasSoftFailure() const {
        return _hasSoftFailure;
    }
    double coveredPercent() const {
        return _coveredPercent;
    }
    const std::vector<int64_t> &getSoftFailureCodes() const {
        return _softFailureCodes;
    }

private:
    static bool parseSoftFailure(const SqlAccessLog &accessLog);
    static double parseCoveredPercent(const multi_call::GigStreamRpcInfoMap &rpcInfoMap);
    static std::vector<int64_t> parseSoftFailureCodes(const SqlAccessLog &accessLog);

private:
    const SqlAccessLog &_accessLog;
    bool _hasSoftFailure;
    double _coveredPercent;
    std::vector<int64_t> _softFailureCodes;
};

} // namespace sql
