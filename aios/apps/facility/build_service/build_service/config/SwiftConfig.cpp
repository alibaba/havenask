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
#include "build_service/config/SwiftConfig.h"

#include <regex>

#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/ProtoUtil.h"

using namespace std;
using namespace build_service::proto;

using autil::legacy::Any;
using autil::legacy::json::JsonMap;

namespace build_service { namespace config {
BS_LOG_SETUP(config, SwiftConfig);

const string SwiftConfig::DEFAULT_SWIFT_WRITER_CONFIG = "functionChain=hashId2partId;mode=async|safe";

SwiftConfig::SwiftConfig() : _readerConfigStr(""), _writerConfigStr(DEFAULT_SWIFT_WRITER_CONFIG) {}

SwiftConfig::~SwiftConfig() {}

SwiftConfig::SwiftConfig(const SwiftConfig& other)
    : _readerConfigStr(other._readerConfigStr)
    , _writerConfigStr(other._writerConfigStr)
    , _swiftClientConfigStr(other._swiftClientConfigStr)
    , _topicConfigMap(other._topicConfigMap)
    , _fullBrokerTopicConfig(other._fullBrokerTopicConfig)
    , _incBrokerTopicConfig(other._incBrokerTopicConfig)
    , _defaultBrokerTopicConfig(other._defaultBrokerTopicConfig)
{
}

SwiftConfig& SwiftConfig::operator=(const SwiftConfig& other)
{
    _readerConfigStr = other._readerConfigStr;
    _writerConfigStr = other._writerConfigStr;
    _swiftClientConfigStr = other._swiftClientConfigStr;
    _topicConfigMap = other._topicConfigMap;
    _fullBrokerTopicConfig = other._fullBrokerTopicConfig;
    _incBrokerTopicConfig = other._incBrokerTopicConfig;
    _defaultBrokerTopicConfig = other._defaultBrokerTopicConfig;
    return *this;
}

void SwiftConfig::jsonizeLegacyTopicConfig(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == FROM_JSON) {
        JsonMap jsonMap = json.GetMap();
        auto iter = jsonMap.find(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _defaultBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG, *_defaultBrokerTopicConfig.get());
        }
        iter = jsonMap.find(FULL_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _fullBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(FULL_SWIFT_BROKER_TOPIC_CONFIG, *_fullBrokerTopicConfig.get());
        }
        iter = jsonMap.find(INC_SWIFT_BROKER_TOPIC_CONFIG);
        if (iter != jsonMap.end()) {
            _incBrokerTopicConfig.reset(new SwiftTopicConfig());
            json.Jsonize(INC_SWIFT_BROKER_TOPIC_CONFIG, *_incBrokerTopicConfig.get());
        }
    } else {
        if (_defaultBrokerTopicConfig) {
            json.Jsonize(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG, *_defaultBrokerTopicConfig.get());
        }
        if (_fullBrokerTopicConfig) {
            json.Jsonize(FULL_SWIFT_BROKER_TOPIC_CONFIG, *_fullBrokerTopicConfig.get());
        }
        if (_incBrokerTopicConfig) {
            json.Jsonize(INC_SWIFT_BROKER_TOPIC_CONFIG, *_incBrokerTopicConfig.get());
        }
    }
}

void SwiftConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(SWIFT_READER_CONFIG, _readerConfigStr, _readerConfigStr);
    json.Jsonize(SWIFT_WRITER_CONFIG, _writerConfigStr, _writerConfigStr);
    json.Jsonize(SWIFT_CLIENT_CONFIG, _swiftClientConfigStr, _swiftClientConfigStr);
    jsonizeLegacyTopicConfig(json);
    jsonizeTopicConfigs(json);
}

void SwiftConfig::jsonizeTopicConfigs(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == FROM_JSON) {
        auto jsonMap = json.GetMap();
        auto iter = jsonMap.find(SWIFT_TOPIC_CONFIGS);
        if (iter == jsonMap.end()) {
            return;
        }
        JsonMap* topicsMap = autil::legacy::AnyCast<JsonMap>(&(iter->second));
        if (topicsMap == NULL) {
            return;
        }
        for (auto iter = topicsMap->begin(); iter != topicsMap->end(); iter++) {
            string name = iter->first;
            auto any = iter->second;
            SwiftTopicConfigPtr topicConfig(new SwiftTopicConfig);
            FromJson(*topicConfig, any);
            _topicConfigMap[name] = topicConfig;
        }
    } else {
        if (_topicConfigMap.size() == 0) {
            return;
        }
        JsonMap topicConfigs;
        for (auto iter = _topicConfigMap.begin(); iter != _topicConfigMap.end(); iter++) {
            topicConfigs[iter->first] = ToJson(*(iter->second));
        }
        json.Jsonize(SWIFT_TOPIC_CONFIGS, topicConfigs);
    }
}

bool SwiftConfig::validate() const
{
    if (_defaultBrokerTopicConfig) {
        if (_fullBrokerTopicConfig || _incBrokerTopicConfig) {
            BS_LOG(ERROR, "once specify [%s] config, cannot specify other topic configs",
                   DEFAULT_SWIFT_BROKER_TOPIC_CONFIG.c_str());
            return false;
        }
        return _defaultBrokerTopicConfig->validate();
    }

    if (!_fullBrokerTopicConfig && !_incBrokerTopicConfig) {
        // specify no topic config, use default.
        return true;
    }

    if (!_fullBrokerTopicConfig || !_incBrokerTopicConfig) {
        BS_LOG(ERROR, "[%s] and [%s] should be specified in pairs.", FULL_SWIFT_BROKER_TOPIC_CONFIG.c_str(),
               INC_SWIFT_BROKER_TOPIC_CONFIG.c_str());
        return false;
    }
    return _fullBrokerTopicConfig->validate(true) && _incBrokerTopicConfig->validate();
}

const SwiftTopicConfigPtr& SwiftConfig::getTopicConfig(const std::string& configName) const
{
    string innerConfigName = configName;
    if (configName.empty()) {
        innerConfigName = DEFAULT_SWIFT_BROKER_TOPIC_CONFIG;
    }

    auto iter = _topicConfigMap.find(innerConfigName);
    if (iter != _topicConfigMap.end()) {
        return iter->second;
    }
    if (configName == FULL_SWIFT_BROKER_TOPIC_CONFIG && _fullBrokerTopicConfig) {
        return _fullBrokerTopicConfig;
    }
    if (configName == INC_SWIFT_BROKER_TOPIC_CONFIG && _incBrokerTopicConfig) {
        return _incBrokerTopicConfig;
    }
    BS_LOG(WARN, "no topic config name [%s], use default topic config", configName.c_str());
    iter = _topicConfigMap.find(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG);
    if (iter != _topicConfigMap.end()) {
        return iter->second;
    }
    if (!_defaultBrokerTopicConfig) {
        _defaultBrokerTopicConfig.reset(new SwiftTopicConfig());
    }
    return _defaultBrokerTopicConfig;
}

bool SwiftConfig::hasTopicConfig(const std::string& configName) const
{
    if (configName.empty() || configName == DEFAULT_SWIFT_BROKER_TOPIC_CONFIG) {
        return true;
    }
    if (_topicConfigMap.find(configName) != _topicConfigMap.end()) {
        return true;
    }
    if (configName == FULL_SWIFT_BROKER_TOPIC_CONFIG && _fullBrokerTopicConfig) {
        return true;
    }
    if (configName == INC_SWIFT_BROKER_TOPIC_CONFIG && _incBrokerTopicConfig) {
        return true;
    }
    return false;
}

const SwiftTopicConfigPtr& SwiftConfig::getFullBrokerTopicConfig() const
{
    return getTopicConfig(FULL_SWIFT_BROKER_TOPIC_CONFIG);
}

const SwiftTopicConfigPtr& SwiftConfig::getIncBrokerTopicConfig() const
{
    return getTopicConfig(INC_SWIFT_BROKER_TOPIC_CONFIG);
}

const SwiftTopicConfigPtr& SwiftConfig::getDefaultTopicConfig() const
{
    return getTopicConfig(DEFAULT_SWIFT_BROKER_TOPIC_CONFIG);
}

string SwiftConfig::getSwiftWriterConfig(uint64_t swiftWriterMaxBufferSize, const string& configName) const
{
    regex pattern(R"(maxBufferSize=\d+)");
    std::smatch m;
    string replaceStr = "maxBufferSize=" + autil::StringUtil::toString(swiftWriterMaxBufferSize);
    const std::string& writerConfigStr = getSwiftWriterConfig(configName);
    if (regex_search(writerConfigStr, m, pattern)) {
        return regex_replace(writerConfigStr, pattern, replaceStr);
    } else {
        return replaceStr + ";" + writerConfigStr;
    }
}

const std::string& SwiftConfig::getSwiftReaderConfig(const std::string& configName) const
{
    auto topicConfig = getTopicConfig(configName);
    if (topicConfig && !topicConfig->readerConfigStr.empty()) {
        return topicConfig->readerConfigStr;
    }
    return _readerConfigStr;
}

const std::string& SwiftConfig::getSwiftWriterConfig(const std::string& configName) const
{
    auto topicConfig = getTopicConfig(configName);
    if (topicConfig && !topicConfig->writerConfigStr.empty()) {
        return topicConfig->writerConfigStr;
    }
    return _writerConfigStr;
}

const std::string& SwiftConfig::getSwiftClientConfig(const std::string& configName) const
{
    auto topicConfig = getTopicConfig(configName);
    if (topicConfig && !topicConfig->swiftClientConfigStr.empty()) {
        return topicConfig->swiftClientConfigStr;
    }
    return _swiftClientConfigStr;
}
std::string SwiftConfig::getProcessorWriterName(uint32_t from, uint32_t to)
{
    return "processor_" + autil::StringUtil::toString(from) + "_" + autil::StringUtil::toString(to);
}

}} // namespace build_service::config
