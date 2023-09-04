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

#include <map>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "swift/common/Common.h"

namespace swift {
namespace config {

class ConfigReader {
public:
    typedef std::map<std::string, std::string> Key2ValMap;
    typedef std::map<std::string, Key2ValMap> Sec2Key2ValMap;

public:
    ConfigReader();
    ~ConfigReader();

public:
    bool read(const std::string &filePath);
    std::string get(const std::string &section, const std::string &option, const std::string &defVal = "");
    int32_t getInt32(const std::string &section, const std::string &option, int32_t defVal = -1);
    uint32_t getUint32(const std::string &section, const std::string &option, uint32_t defVal = 0);
    int64_t getInt64(const std::string &section, const std::string &option, int64_t defVal = -1);

    double getDouble(const std::string &section, const std::string &option, double defVal = 0.0f);
    bool getBool(const std::string &section, const std::string &option, bool defVal = false);

    uint16_t getUint16(const std::string &section, const std::string &option, uint16_t defVal = 0);

    template <typename T>
    bool getOption(const std::string &section, const std::string &option, T &value);

    Key2ValMap getKVByPrefix(const std::string &section, const std::string &prefix) const;

    const Key2ValMap items(const std::string &section) const;
    bool hasSection(const std::string &section) const;
    bool hasOption(const std::string &section, const std::string &option);
    bool getContentByPath(const std::string &path, std::string &content);

private:
    bool parseContent(const std::string &configStr);
    void addSection(const std::string &section, Key2ValMap &kvMap);

private:
    Sec2Key2ValMap _sec2KeyValMap;

private:
    friend class BrokerConfigParserTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ConfigReader);
////////////////////////////////////////////////////////////////////
template <typename T>
bool ConfigReader::getOption(const std::string &section, const std::string &option, T &value) {
    if (!hasOption(section, option)) {
        return false;
    }
    const std::string &valueStr = _sec2KeyValMap[section][option];
    if (!autil::StringUtil::fromString(valueStr, value)) {
        return false;
    }
    return true;
}

} // namespace config
} // namespace swift
