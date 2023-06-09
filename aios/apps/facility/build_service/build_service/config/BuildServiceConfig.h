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
#ifndef ISEARCH_BS_BUILDSERVICECONFIG_H
#define ISEARCH_BS_BUILDSERVICECONFIG_H

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class BuildServiceConfig : public autil::legacy::Jsonizable
{
public:
    BuildServiceConfig();
    ~BuildServiceConfig();
    BuildServiceConfig(const BuildServiceConfig&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    bool operator==(const BuildServiceConfig& other) const;
    bool validate() const;
    std::string getApplicationId() const;
    std::string getMessageIndexName() const;
    std::string getBuilderIndexRoot(bool isFullBuilder) const;
    void setIndexRoot(const std::string& indexRoot) { _indexRoot = indexRoot; }
    std::string getIndexRoot() const;
    std::string getSwiftRoot(bool isFull) const;

public:
    uint16_t amonitorPort;
    std::string userName;
    std::string serviceName;
    std::string hippoRoot;
    std::string zkRoot;
    // clusterName : zkRoot
    std::map<std::string, std::string> suezVersionZkRoots;
    std::string heartbeatType;
    CounterConfig counterConfig;

private:
    std::string _swiftRoot;
    std::string _fullSwiftRoot;
    std::string _fullBuilderTmpRoot;
    std::string _indexRoot;

private:
    static const uint16_t DEFAULT_AMONITOR_PORT = 10086;
    static const std::string DEFAULT_MESSAGE_INDEX_NAME;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildServiceConfig);

}} // namespace build_service::config

#endif // ISEARCH_BS_BUILDSERVICECONFIG_H
