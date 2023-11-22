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
#include <stddef.h>
#include <string>
#include <vector>

#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "autil/CompressionUtil.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "navi/engine/NaviResult.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/data/ErrorResult.h"
#include "table/Table.h"

namespace sql {

class QrsSessionSqlResult {
public:
    enum SqlResultFormatType {
        SQL_RF_JSON = 0,
        SQL_RF_STRING = 1,
        SQL_RF_FULL_JSON = 2,
        SQL_RF_TSDB = 3,
        SQL_RF_FLATBUFFERS = 4,
        SQL_RF_TSDB_FLATBUFFERS = 6,
        SQL_RF_TABLE = 7,
        SQL_RF_FLATBUFFERS_TIMELINE = 8,
        SQL_RF_FLATBUFFERS_BYTE = 9,
        SQL_RF_FLATBUFFERS_BYTE_TIMELINE = 10,
    };
    enum SearchInfoLevel {
        SQL_SIL_UNKNOWN = 0,
        SQL_SIL_NONE,
        SQL_SIL_SIMPLE,
        SQL_SIL_FULL,
    };

public:
    QrsSessionSqlResult(SqlResultFormatType formatType_ = SQL_RF_JSON)
        : multicallEc(multi_call::MULTI_CALL_ERROR_RPC_FAILED)
        , formatType(formatType_)
        , searchInfoLevel(SQL_SIL_NONE)
        , resultCompressType(autil::CompressType::NO_COMPRESS)
        , allowSoftFailure(false) {}
    static SqlResultFormatType strToType(const std::string &format) {
        if (format == "json") {
            return QrsSessionSqlResult::SQL_RF_JSON;
        } else if (format == "full_json") {
            return QrsSessionSqlResult::SQL_RF_FULL_JSON;
        } else if (format == "khronos" || format == "tsdb") {
            return QrsSessionSqlResult::SQL_RF_TSDB;
        } else if (format == "flatbuffers") {
            return QrsSessionSqlResult::SQL_RF_FLATBUFFERS;
        } else if (format == "flatbuffers_byte") {
            return QrsSessionSqlResult::SQL_RF_FLATBUFFERS_BYTE;
        } else if (format == "flatbuffers_byte_timeline") {
            return QrsSessionSqlResult::SQL_RF_FLATBUFFERS_BYTE_TIMELINE;
        } else if (format == "flatbuffers_timeline") {
            return QrsSessionSqlResult::SQL_RF_FLATBUFFERS_TIMELINE;
        } else if (format == "tsdbfb") {
            return QrsSessionSqlResult::SQL_RF_TSDB_FLATBUFFERS;
        } else if (format == "table") {
            return QrsSessionSqlResult::SQL_RF_TABLE;
        } else {
            return QrsSessionSqlResult::SQL_RF_STRING;
        }
    }

    static std::string typeToStr(SqlResultFormatType type) {
        switch (type) {
        case SQL_RF_JSON:
            return "json";
        case SQL_RF_FULL_JSON:
            return "full_json";
        case SQL_RF_STRING:
            return "string";
        case SQL_RF_TSDB:
            return "tsdb";
        case SQL_RF_FLATBUFFERS:
            return "flatbuffers";
        case SQL_RF_FLATBUFFERS_BYTE:
            return "flatbuffers_byte";
        case SQL_RF_FLATBUFFERS_BYTE_TIMELINE:
            return "flatbuffers_byte_timeline";
        case SQL_RF_FLATBUFFERS_TIMELINE:
            return "flatbuffers_timeline";
        case SQL_RF_TSDB_FLATBUFFERS:
            return "tsdbfb";
        case SQL_RF_TABLE:
            return "table";
        default:
            return "string";
        }
    }
    size_t getTableRowCount() const {
        return table ? table->getRowCount() : 0;
    }

public:
    multi_call::MultiCallErrorCode multicallEc;
    SqlResultFormatType formatType;
    ErrorResult errorResult;
    table::TablePtr table;
    std::string resultStr;
    std::string formatDesc;
    std::vector<std::string> sqlTrace;
    std::string sqlQuery;
    std::string naviGraph;
    iquan::IquanDqlResponseWrapper iquanResponseWrapper;
    SearchInfoLevel searchInfoLevel;
    autil::CompressType resultCompressType;
    bool allowSoftFailure;
    iquan::IquanDqlRequest sqlQueryRequest;
};

typedef std::shared_ptr<QrsSessionSqlResult> QrsSessionSqlResultPtr;
} // namespace sql
