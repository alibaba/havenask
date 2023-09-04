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
#include "suez/drc/LogRecordBuilder.h"

namespace suez {

LogRecordBuilder::LogRecordBuilder() {}

LogRecordBuilder::~LogRecordBuilder() {}

void LogRecordBuilder::addField(const autil::StringView &key, const autil::StringView &value) {
    _fieldGroup.addProductionField(key, value);
}

bool LogRecordBuilder::finalize(LogRecord &log) {
    auto str = _fieldGroup.toString();
    return log.parse(std::move(str));
}

bool LogRecordBuilder::makeInsert(std::map<std::string, std::string> fieldMap, LogRecord &log) {
    fieldMap.emplace(LogRecord::LOG_TYPE_FIELD, "add");
    return makeLogRecord(std::move(fieldMap), log);
}

bool LogRecordBuilder::makeUpdate(std::map<std::string, std::string> fieldMap, LogRecord &log) {
    fieldMap.emplace(LogRecord::LOG_TYPE_FIELD, "update_field");
    return makeLogRecord(std::move(fieldMap), log);
}

bool LogRecordBuilder::makeDelete(std::map<std::string, std::string> fieldMap, LogRecord &log) {
    fieldMap.emplace(LogRecord::LOG_TYPE_FIELD, "delete");
    return makeLogRecord(std::move(fieldMap), log);
}

bool LogRecordBuilder::makeAlter(std::map<std::string, std::string> fieldMap, LogRecord &log) {
    fieldMap.emplace(LogRecord::LOG_TYPE_FIELD, "alter");
    return makeLogRecord(std::move(fieldMap), log);
}

bool LogRecordBuilder::makeLogRecord(std::map<std::string, std::string> fieldMap, LogRecord &log) {
    LogRecordBuilder builder;
    for (const auto &it : fieldMap) {
        builder.addField(autil::StringView(it.first), autil::StringView(it.second));
    }
    return builder.finalize(log);
}

} // namespace suez
