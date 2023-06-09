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

#include <unordered_map>

#include "autil/StringView.h"

namespace suez {

enum LogType
{
    LT_ADD,
    LT_UPDATE,
    LT_DELETE,
    LT_UNKNOWN
};
const char *LogType_Name(LogType logType);
LogType ParseLogType(const autil::StringView &logTypeStr);

class LogRecord {
public:
    LogRecord();
    explicit LogRecord(int64_t logId);
    ~LogRecord();

public:
    bool init(int64_t logId, std::string data);
    bool parse(std::string data);
    void reset();

    LogType getType() const { return _type; }
    void setLogId(int64_t logId) { _logId = logId; }
    int64_t getLogId() const { return _logId; }
    const std::string &getData() const { return _data; }
    std::string stealData() { return std::move(_data); }

    bool getField(const autil::StringView &key, autil::StringView &value) const {
        auto it = _fieldMap.find(key);
        if (it == _fieldMap.end()) {
            return false;
        }
        value = it->second;
        return true;
    }

    std::string debugString() const;
    std::string debugString(const std::vector<std::string> &fields) const;

private:
    LogType _type = LT_UNKNOWN;
    int64_t _logId = -1;
    std::string _data;
    std::unordered_map<autil::StringView, autil::StringView> _fieldMap;

public:
    static const std::string LOG_TYPE_FIELD;
};

} // namespace suez
