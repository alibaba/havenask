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
#include "swift/config/ConfigReader.h"

#include <iosfwd>
#include <strings.h>
#include <sys/types.h>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, ConfigReader);

ConfigReader::ConfigReader() {}

ConfigReader::~ConfigReader() {}

bool ConfigReader::read(const std::string &filePath) {
    string fileContent;
    if (!getContentByPath(filePath, fileContent)) {
        AUTIL_LOG(ERROR, "Failed to get File:[%s]", filePath.c_str());
        return false;
    }
    return parseContent(fileContent);
}

bool ConfigReader::hasSection(const std::string &section) const {
    return _sec2KeyValMap.find(section) != _sec2KeyValMap.end();
}

bool ConfigReader::hasOption(const std::string &section, const std::string &option) {
    Sec2Key2ValMap::iterator it = _sec2KeyValMap.find(section);
    if (it != _sec2KeyValMap.end()) {
        if (it->second.find(option) != it->second.end()) {
            return true;
        }
    }
    return false;
}

string ConfigReader::get(const string &section, const string &option, const string &defVal) {
    if (hasOption(section, option)) {
        return _sec2KeyValMap[section][option];
    }
    return defVal;
}

int64_t ConfigReader::getInt64(const std::string &section, const std::string &option, int64_t defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        int64_t val;
        if (autil::StringUtil::fromString(valStr, val)) {
            return val;
        }
    }
    return defVal;
}

int32_t ConfigReader::getInt32(const string &section, const string &option, int32_t defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        int32_t val;
        if (autil::StringUtil::strToInt32(valStr.c_str(), val)) {
            return val;
        }
    }
    return defVal;
}

uint32_t ConfigReader::getUint32(const string &section, const string &option, uint32_t defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        uint32_t val;
        if (autil::StringUtil::strToUInt32(valStr.c_str(), val)) {
            return val;
        }
    }
    return defVal;
}

uint16_t ConfigReader::getUint16(const string &section, const string &option, uint16_t defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        uint16_t val;
        if (autil::StringUtil::strToUInt16(valStr.c_str(), val)) {
            return val;
        }
    }

    return defVal;
}

double ConfigReader::getDouble(const string &section, const string &option, double defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        double val;
        if (autil::StringUtil::strToDouble(valStr.c_str(), val)) {
            return val;
        }
    }
    return defVal;
}

bool ConfigReader::getBool(const string &section, const string &option, bool defVal) {
    if (hasOption(section, option)) {
        string valStr = _sec2KeyValMap[section][option];
        if (!strcasecmp(valStr.c_str(), "true")) {
            return true;
        } else if (!strcasecmp(valStr.c_str(), "false")) {
            return false;
        }
    }
    return defVal;
}

const ConfigReader::Key2ValMap ConfigReader::items(const std::string &section) const {
    auto iterator = _sec2KeyValMap.find(section);
    if (iterator != _sec2KeyValMap.end()) {
        return iterator->second;
    }
    return Key2ValMap();
}

bool ConfigReader::getContentByPath(const string &path, string &content) {
    File *file = FileSystem::openFile(path, READ);
    if (!file) {
        AUTIL_LOG(ERROR, "Failed to Open File [%s] To Read", path.c_str());
        return false;
    }
    if (!file->isOpened()) {
        AUTIL_LOG(ERROR, "Failed to Open File [%s] To Read", path.c_str());
        DELETE_AND_SET_NULL(file);
        return false;
    }
    file->seek(0, FILE_SEEK_END);
    int64_t fileLength = file->tell();
    file->seek(0, FILE_SEEK_SET);
    char *buf = new char[fileLength];
    ssize_t size = file->read(buf, fileLength);
    if (size == -1 || (int64_t)size != fileLength) {
        AUTIL_LOG(ERROR, "Read File [%s] Length Error", path.c_str());
        SWIFT_DELETE_ARRAY(buf);
        DELETE_AND_SET_NULL(file);
        return false;
    }
    file->close();
    DELETE_AND_SET_NULL(file);
    content.assign(buf, fileLength);
    SWIFT_DELETE_ARRAY(buf);
    return true;
}

void ConfigReader::addSection(const std::string &section, Key2ValMap &kvMap) {
    Sec2Key2ValMap::iterator it = _sec2KeyValMap.find(section);
    if (it != _sec2KeyValMap.end()) {
        Key2ValMap &tmpMap = it->second;
        tmpMap.insert(kvMap.begin(), kvMap.end());
    } else {
        _sec2KeyValMap[section] = kvMap;
    }
}

bool ConfigReader::parseContent(const std::string &configStr) {
    string key, val, sec, line;
    Key2ValMap kvMap;
    vector<string>::size_type size = 0;
    int32_t status = 0;

    vector<string> lines = autil::StringUtil::split(configStr, "\n");
    if (lines.size() == 0) {
        AUTIL_LOG(WARN, "Content in Config is NULL");
        return false;
    }

    for (size = 0; size < lines.size(); ++size) {
        line = lines[size];
        autil::StringUtil::trim(line);
        if (line.size() != 0 && line[0] == '[' && line[line.size() - 1] == ']') {
            if (!sec.empty()) {
                addSection(sec, kvMap);
                kvMap.clear();
            }
            sec = line.substr(1, line.size() - 2);
            status = 1;
            if (sec.empty()) {
                AUTIL_LOG(ERROR, "Section is NULL");
                return false;
            }
        } else if (line.size() != 0 && line[0] != '#' && status == 1) {
            string::size_type pos = line.find("=");
            if (pos != string::npos) {
                key = line.substr(0, pos);
                val = line.substr(pos + 1, line.size() - pos - 1);
                autil::StringUtil::trim(key);
                autil::StringUtil::trim(val);
                if (key.empty()) {
                    AUTIL_LOG(WARN, "Key is NULL");
                    return false;
                }
                kvMap[key] = val;
            } else {
                AUTIL_LOG(ERROR, "Error in line [%s]", line.c_str());
                return false;
            }
        } else if (line.size() != 0 && line[0] != '#') {
            AUTIL_LOG(ERROR, "Absent Section before line [%s]", line.c_str());
            return false;
        }
    }
    addSection(sec, kvMap);
    return true;
}

ConfigReader::Key2ValMap ConfigReader::getKVByPrefix(const string &section, const string &prefix) const {
    Key2ValMap output;
    const Key2ValMap &kvMap = items(section);
    for (const auto &iterator : kvMap) {
        if (iterator.first.find(prefix) == 0) {
            output[iterator.first] = iterator.second;
        }
    }
    return output;
}

} // namespace config
} // namespace swift
