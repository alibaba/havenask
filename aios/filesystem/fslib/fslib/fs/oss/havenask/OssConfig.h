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
#ifndef FSLIB_PLUGIN_OSSCONFIG_H
#define FSLIB_PLUGIN_OSSCONFIG_H

#include <fslib/common.h>
#include <oss_c_sdk/aos_http_io.h>
#include <oss_c_sdk/oss_api.h>
#include <string>

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);

class OssConfig {
public:
    OssConfig(const std::string &configPath);

public:
    const std::string &getOssAccessId() const;

    const std::string &getOssAccessKey() const;

    const std::string &getOssEndpoint() const;

    bool readConfig();

private:
    bool readConfig(std::map<std::string, std::map<std::string, std::string> > &params);
    std::string getConfigValue(const std::map<std::string, std::map<std::string, std::string> > &params,
                               const std::string& rootKey,
                               const std::string &subKey,
                               const std::string &defalutValue) const;
private:
    std::string _ossAccessId;
    std::string _ossAccessKey;
    std::string _ossEndpoint;

    std::string _configPath;
};

FSLIB_PLUGIN_END_NAMESPACE(oss);

#endif // FSLIB_PLUGIN_OSSCONFIG_H
