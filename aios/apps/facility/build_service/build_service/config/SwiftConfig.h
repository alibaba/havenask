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

#include "autil/legacy/jsonizable.h"
#include "build_service/common_define.h"
#include "build_service/config/SwiftTopicConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class SwiftConfig : public autil::legacy::Jsonizable
{
public:
    SwiftConfig();
    ~SwiftConfig();
    SwiftConfig(const SwiftConfig&);

public:
    SwiftConfig& operator=(const SwiftConfig&);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;
    bool hasTopicConfig(const std::string& configName) const;
    const SwiftTopicConfigPtr& getTopicConfig(const std::string& configName) const;
    const SwiftTopicConfigPtr& getDefaultTopicConfig() const;

    const std::string& getSwiftReaderConfig(const std::string& configName) const;
    const std::string& getSwiftWriterConfig(const std::string& configName) const;
    std::string getSwiftWriterConfig(uint64_t swiftWriterMaxBufferSize, const std::string& configName) const;
    const std::string& getSwiftClientConfig(const std::string& configName) const;
    const SwiftTopicConfigPtr& getFullBrokerTopicConfig() const;
    const SwiftTopicConfigPtr& getIncBrokerTopicConfig() const;
    bool isFullIncShareBrokerTopic() const { return !_fullBrokerTopicConfig && !_incBrokerTopicConfig; }

public:
    static std::string getProcessorWriterName(uint32_t from, uint32_t to);

private:
    void jsonizeLegacyTopicConfig(autil::legacy::Jsonizable::JsonWrapper& json);
    void jsonizeTopicConfigs(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    std::string _readerConfigStr;
    std::string _writerConfigStr;
    std::string _swiftClientConfigStr;
    std::map<std::string, SwiftTopicConfigPtr> _topicConfigMap;
    SwiftTopicConfigPtr _fullBrokerTopicConfig;
    SwiftTopicConfigPtr _incBrokerTopicConfig;
    mutable SwiftTopicConfigPtr _defaultBrokerTopicConfig;

private:
    static const std::string DEFAULT_SWIFT_WRITER_CONFIG;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SwiftConfig);

}} // namespace build_service::config
