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
#ifndef ISEARCH_RESOURCE_READER_CONFIGPARSER_H
#define ISEARCH_RESOURCE_READER_CONFIGPARSER_H

#include "autil/Log.h"
#include "autil/CommonMacros.h"
#include "autil/legacy/json.h"

namespace resource_reader {

class ConfigParser
{

public:
    static const std::string INHERIT_FROM_KEY;
public:
    enum ParseResult {
        PR_OK,
        PR_FAIL,
        PR_SECTION_NOT_EXIST
    };
public:
    ConfigParser(const std::string &basePath);
    ~ConfigParser();
private:
    ConfigParser(const ConfigParser &);
    ConfigParser& operator=(const ConfigParser &);
public:
    bool parse(const std::string &relativePath,
               autil::legacy::json::JsonMap &jsonMap) const;
private:
    bool merge(autil::legacy::json::JsonMap &jsonMap) const;
    bool mergeJsonMap(const autil::legacy::json::JsonMap &src,
                      autil::legacy::json::JsonMap &dst) const;
    bool mergeJsonArray(autil::legacy::json::JsonArray &dst) const;
    bool mergeWithInherit(autil::legacy::json::JsonMap &jsonMap) const;
private:
    std::string _basePath;
    mutable std::vector<std::string> _configFileNames; // for inherit loop detection
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_RESOURCE_READER_CONFIGPARSER_H
