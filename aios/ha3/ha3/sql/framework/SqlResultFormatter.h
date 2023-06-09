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

#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace isearch {
namespace sql {
class QrsSessionSqlResult;
class SqlAccessLogFormatHelper;
class SqlSearchInfo;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class SqlResultFormatter {
public:
    SqlResultFormatter() {}
    ~SqlResultFormatter() {}

public:
    static void format(QrsSessionSqlResult &sqlResult,
                       const SqlAccessLogFormatHelper *accessLogHelper,
                       autil::mem_pool::Pool *pool = nullptr);
    static void formatJson(QrsSessionSqlResult &sqlResult,
                           const SqlAccessLogFormatHelper *accessLogHelper);
    static void formatFullJson(QrsSessionSqlResult &sqlResult,
                               const SqlAccessLogFormatHelper *accessLogHelper);
    static void formatString(QrsSessionSqlResult &sqlResult,
                             const SqlAccessLogFormatHelper *accessLogHelper);
    static void formatTsdb(QrsSessionSqlResult &sqlResult,
                           const SqlAccessLogFormatHelper *accessLogHelper);
    static void formatFlatbuffers(QrsSessionSqlResult &sqlResult,
                                  const SqlAccessLogFormatHelper *accessLogHelper,
                                  autil::mem_pool::Pool *pool,
                                  bool timeline = false,
                                  bool useByteString = false);
    static void formatTsdbFlatbuffers(QrsSessionSqlResult &sqlResult,
                                      const SqlAccessLogFormatHelper *accessLogHelper,
                                      autil::mem_pool::Pool *pool);
    static void formatTable(QrsSessionSqlResult &sqlResult,
                            const SqlAccessLogFormatHelper *accessLogHelper,
                            autil::mem_pool::Pool *pool);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
