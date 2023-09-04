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
#include "sql/framework/ResultFormatter.h"

#include "sql/framework/QrsSessionSqlResult.h"
#include "sql/framework/SqlResultFormatter.h"

namespace sql {

AUTIL_LOG_SETUP(sql, ResultFormatter);

void ResultFormatter::format(QrsSessionSqlResult &sqlResult,
                             const SqlAccessLogFormatHelper *accessLogHelper,
                             autil::mem_pool::Pool *pool) {
    switch (sqlResult.formatType) {
    case QrsSessionSqlResult::SQL_RF_JSON:
        SqlResultFormatter::formatJson(sqlResult, accessLogHelper);
        return;
    case QrsSessionSqlResult::SQL_RF_FULL_JSON:
        SqlResultFormatter::formatFullJson(sqlResult, accessLogHelper);
        return;
    case QrsSessionSqlResult::SQL_RF_TSDB:
        SQL_LOG(ERROR, "not support format type: tsdb");
        return;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS:
        SqlResultFormatter::formatFlatbuffers(sqlResult, accessLogHelper, pool, false, false);
        return;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS_BYTE:
        SqlResultFormatter::formatFlatbuffers(sqlResult, accessLogHelper, pool, false, true);
        return;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS_BYTE_TIMELINE:
        SqlResultFormatter::formatFlatbuffers(sqlResult, accessLogHelper, pool, true, true);
        return;
    case QrsSessionSqlResult::SQL_RF_FLATBUFFERS_TIMELINE:
        SqlResultFormatter::formatFlatbuffers(sqlResult, accessLogHelper, pool, true, false);
        return;
    case QrsSessionSqlResult::SQL_RF_TSDB_FLATBUFFERS:
        SQL_LOG(ERROR, "not support format type: tsdb flatbuffers");
        return;
    case QrsSessionSqlResult::SQL_RF_TABLE:
        SqlResultFormatter::formatTable(sqlResult, accessLogHelper, pool);
        return;
    default:
        SqlResultFormatter::formatString(sqlResult, accessLogHelper);
        return;
    }
}

} // namespace sql
