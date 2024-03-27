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
#include "fslib/fs/oss/havenask/OssConfig.h"
#include "autil/StringUtil.h"
#include <fstream>

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);
AUTIL_DECLARE_AND_SETUP_LOGGER(oss, OssFile);

using namespace std;

OssConfig::OssConfig(const string &configPath) : _configPath(configPath) {}

bool OssConfig::readConfig() {
    map<string, map<string, string> > params;
    if (!readConfig(params)) {
        AUTIL_LOG(ERROR, "read file %s failed", _configPath.c_str());
        return false;
    }
    _ossEndpoint = getConfigValue(params, "credentials", "endpoint", "");
    _ossAccessId = getConfigValue(params, "credentials", "accesskeyid", "");
    _ossAccessKey = getConfigValue(params, "credentials", "accesskeysecret", "");
    if (_ossAccessId == "" || _ossAccessKey == "" || _ossEndpoint == "") {
        AUTIL_LOG(ERROR,
                  "accessid %s, accesskey %s, host %s is empty",
                  _ossAccessId.c_str(),
                  _ossAccessKey.c_str(),
                  _ossEndpoint.c_str());
        return false;
    }
    return true;
}

bool OssConfig::readConfig(map<string, map<string, string> > &params) {
    ifstream configFile(_configPath);
    if (!configFile.is_open()) {
        AUTIL_LOG(ERROR, "open oss config file[%s] failed.", _configPath.c_str());
        return false;
    }
    string line;
    string rootKey;
    while(std::getline(configFile, line)) {
        autil::StringUtil::trim(line);
        if (line.empty() || line[0] == ';' || line[0] == '#') {
            continue;
        }
        if (autil::StringUtil::startsWith(line, "[")
            && autil::StringUtil::endsWith(line, "]"))
        {
            rootKey = line.substr(1, line.size() - 2);
            autil::StringUtil::toLowerCase(rootKey);
            AUTIL_LOG(ERROR, "root key:%s", rootKey.c_str());
            continue;
        }
        if (!rootKey.empty()) {
            auto pos = line.find("=");
            if (string::npos != pos) {
                string key = line.substr(0, pos);
                string value = line.substr(pos + 1, line.size() - 1);
                autil::StringUtil::trim(key);
                autil::StringUtil::toLowerCase(key);
                autil::StringUtil::trim(value);
                autil::StringUtil::toLowerCase(value);
                params[rootKey][key] = value;
                AUTIL_LOG(ERROR, "key:%s value:%s", key.c_str(), value.c_str());
            }
        }
    }
    configFile.close();
    return true;
}

string OssConfig::getConfigValue(const map<string, map<string, string> > &params,
                                 const string& rootKey,
                                 const string &subKey,
                                 const string &defalutValue) const
{
    AUTIL_LOG(ERROR, "rootKey:%s subKey:%s", rootKey.c_str(), subKey.c_str());
    AUTIL_LOG(ERROR, "params size:%lu", params.size());
    auto it = params.find(rootKey);
    if (it != params.end()) {
        AUTIL_LOG(ERROR, "params sub size:%lu", it->second.size());
        auto sIt = it->second.find(subKey);
        if (sIt != it->second.end()) {
            return sIt->second;
        }
    }
    return defalutValue;
}

const string &OssConfig::getOssAccessId() const { return _ossAccessId; }
const string &OssConfig::getOssAccessKey() const { return _ossAccessKey; }
const string &OssConfig::getOssEndpoint() const { return _ossEndpoint; }
FSLIB_PLUGIN_END_NAMESPACE(oss);
