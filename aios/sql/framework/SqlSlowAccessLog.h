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

#include <memory>

#include "autil/Log.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/framework/SqlAccessLog.h"

namespace sql {

class SqlSlowAccessLog {
public:
    SqlSlowAccessLog(const SqlAccessInfo &info)
        : _info(info) {}
    ~SqlSlowAccessLog();

private:
    void log();

private:
    SqlAccessInfo _info;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlSlowAccessLog> SqlSlowAccessLogPtr;
} // namespace sql
