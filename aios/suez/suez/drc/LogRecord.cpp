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
#include "suez/drc/LogRecord.h"

#include <cassert>
#include <sstream>

#include "autil/Log.h"
#include "swift/common/FieldGroupReader.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez.drc, LogRecord);

const std::string LogRecord::LOG_TYPE_FIELD = "CMD";

const char *LogType_Name(LogType logType) {
    switch (logType) {
    case LT_ADD:
        return "add";
    case LT_UPDATE:
        return "update";
    case LT_DELETE:
        return "delete";
    default:
        return "unknown";
    }
}

LogType ParseLogType(const autil::StringView &logTypeStr) {
    if (logTypeStr == "add") {
        return LT_ADD;
    } else if (logTypeStr == "update_field" || logTypeStr == "update") {
        return LT_UPDATE;
    } else if (logTypeStr == "delete") {
        return LT_DELETE;
    } else {
        return LT_UNKNOWN;
    }
}

LogRecord::LogRecord() {}

LogRecord::LogRecord(int64_t logId) : _logId(logId) {}

LogRecord::~LogRecord() {}

bool LogRecord::init(int64_t logId, std::string data) {
    _logId = logId;
    return parse(std::move(data));
}

bool LogRecord::parse(std::string data) {
    _fieldMap.clear();
    _type = LT_UNKNOWN;
    _data = std::move(data);

    swift::common::FieldGroupReader fieldGroup;
    if (!fieldGroup.fromProductionString(_data)) {
        AUTIL_LOG(ERROR, "failed to parse log from swift field message, data: %s", _data.c_str());
        return false;
    }

    for (size_t i = 0; i < fieldGroup.getFieldSize(); ++i) {
        const swift::common::Field *field = fieldGroup.getField(i);
        assert(field != nullptr);
        _fieldMap.emplace(field->name, field->value);
    }

    auto it = _fieldMap.find(autil::StringView(LOG_TYPE_FIELD));
    if (it != _fieldMap.end()) {
        _type = ParseLogType(it->second);
    }

    if (_type == LT_UNKNOWN) {
        AUTIL_LOG(ERROR, "unsupported log: %s", _data.c_str());
        return false;
    }
    return true;
}

void LogRecord::reset() {
    _type = LT_UNKNOWN;
    _logId = -1;
    _fieldMap.clear();
    _data.clear();
}

std::string LogRecord::debugString() const {
    std::stringstream ss;
    ss << "id=" << _logId << "\t";
    ss << "type=" << LogType_Name(_type) << "\t";
    for (const auto &it : _fieldMap) {
        ss << it.first.to_string() << "=" << it.second.to_string() << "\t";
    }
    return ss.str();
}

std::string LogRecord::debugString(const std::vector<std::string> &fields) const {
    std::stringstream ss;
    ss << "id=" << _logId << ",";
    ss << "type=" << LogType_Name(_type) << ",";
    for (auto i = 0; i < fields.size(); ++i) {
        ss << fields[i] << "=";
        autil::StringView value;
        if (getField(autil::StringView(fields[i]), value)) {
            ss << value.to_string();
        }
        if (i + 1 != fields.size()) {
            ss << ",";
        }
    }
    return ss.str();
}

} // namespace suez
