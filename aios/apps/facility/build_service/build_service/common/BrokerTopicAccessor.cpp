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
#include "build_service/common/BrokerTopicAccessor.h"

#include <iosfwd>
#include <memory>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/util/SwiftClientCreator.h"

using namespace std;
using namespace autil;
using namespace build_service::config;

namespace build_service { namespace common {
BS_LOG_SETUP(common, BrokerTopicAccessor);

BrokerTopicAccessor::BrokerTopicAccessor(proto::BuildId buildId) : _buildId(buildId)
{
    _swiftClientCreator.reset(new util::SwiftClientCreator);
}

BrokerTopicAccessor::~BrokerTopicAccessor() {}

std::string BrokerTopicAccessor::getTopicName(const config::ResourceReaderPtr& resourceReader,
                                              const std::string& clusterName, const std::string& topicConfigName,
                                              const std::string& tag)
{
    SwiftAdminFacade facade;
    if (!facade.init(_buildId, resourceReader, clusterName)) {
        return string();
    }
    return facade.getTopicName(topicConfigName, tag);
}

std::string BrokerTopicAccessor::getTopicId(const config::ResourceReaderPtr& resourceReader,
                                            const std::string& clusterName, const std::string& topicConfigName,
                                            const std::string& tag)
{
    SwiftAdminFacade facade;
    if (!facade.init(_buildId, resourceReader, clusterName)) {
        return string();
    }
    return facade.getTopicId(topicConfigName, tag);
}

pair<string, string> BrokerTopicAccessor::registBrokerTopic(const std::string& roleId,
                                                            const ResourceReaderPtr& resourceReader,
                                                            const std::string& clusterName,
                                                            const std::string& topicConfigName, const std::string& tag)
{
    bool safeWrite = false;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), config::SAFE_WRITE, safeWrite)) {
        BS_LOG(ERROR,
               "[%s] regist topic for cluster [%s], topic config [%s], tag [%s] failed, failed to check safe write",
               roleId.c_str(), clusterName.c_str(), topicConfigName.c_str(), tag.c_str());
        return make_pair(string(), string());
    }
    string brokerTopicKey = getTopicName(resourceReader, clusterName, topicConfigName, tag);
    if (brokerTopicKey.empty()) {
        BS_LOG(ERROR, "[%s] regist topic for cluster [%s], topic config [%s], tag [%s] failed", roleId.c_str(),
               clusterName.c_str(), topicConfigName.c_str(), tag.c_str());
        return make_pair(string(), string());
    }
    auto iter = _usingBrokerTopics.find(brokerTopicKey);
    if (iter != _usingBrokerTopics.end()) {
        addApplyId(roleId, brokerTopicKey);
        return make_pair(brokerTopicKey, iter->second->getSwiftRoot());
    }

    BrokerTopicKeeperPtr brokerTopic = createBrokerTopicKeeper();
    if (!brokerTopic->init(_buildId, resourceReader, clusterName, topicConfigName, brokerTopicKey, safeWrite)) {
        BS_LOG(ERROR, "init brokerTopic failed");
        return make_pair(string(), string());
    }
    _usingBrokerTopics[brokerTopicKey] = brokerTopic;
    addApplyId(roleId, brokerTopicKey);
    return make_pair(brokerTopicKey, brokerTopic->getSwiftRoot());
}

void BrokerTopicAccessor::addApplyId(const std::string& roleId, const std::string brokerTopicKey)
{
    auto iter = _brokerTopicApplers.find(brokerTopicKey);
    if (iter == _brokerTopicApplers.end()) {
        _brokerTopicApplers[brokerTopicKey].push_back(roleId);
    } else {
        for (auto& applyRole : iter->second) {
            if (applyRole == roleId) {
                return;
            }
        }
        _brokerTopicApplers[brokerTopicKey].push_back(roleId);
    }
    BS_LOG(INFO, "add applyer [%s] for broker topic [%s]", roleId.c_str(), brokerTopicKey.c_str());
}

void BrokerTopicAccessor::deregistBrokerTopic(const std::string& roleId, const std::string& topicName)
{
    string brokerTopicKey = topicName;
    auto iter = _brokerTopicApplers.find(brokerTopicKey);
    if (iter == _brokerTopicApplers.end()) {
        return;
    }
    for (auto roleIter = iter->second.begin(); roleIter != iter->second.end(); roleIter++) {
        if (*roleIter == roleId) {
            iter->second.erase(roleIter);
            BS_LOG(INFO, "erase applyer [%s] for broker topic [%s]", roleId.c_str(), topicName.c_str());
            if (iter->second.size() == 0) {
                BS_LOG(INFO, "no applyer for broker topic [%s], will delete", topicName.c_str());
                _brokerTopicApplers.erase(iter);
            }
            break;
        }
    }
}

bool BrokerTopicAccessor::prepareBrokerTopics()
{
    bool ret = true;
    for (auto topicIter = _usingBrokerTopics.begin(); topicIter != _usingBrokerTopics.end(); ++topicIter) {
        auto rolesIter = _brokerTopicApplers.find(topicIter->first);
        if (rolesIter == _brokerTopicApplers.end()) {
            continue;
        }
        if (rolesIter->second.size() == 0) {
            continue;
        }
        if (!topicIter->second->prepareBrokerTopic()) {
            ret = false;
        }
    }
    return ret;
}

bool BrokerTopicAccessor::clearUselessBrokerTopics(bool clearAll)
{
    bool ret = true;
    if (clearAll) {
        for (auto topicIter = _usingBrokerTopics.begin(); topicIter != _usingBrokerTopics.end(); ++topicIter) {
            if (!topicIter->second->destroyAllBrokerTopic()) {
                ret = false;
            }
        }
        if (ret) {
            _usingBrokerTopics.clear();
            _brokerTopicApplers.clear();
        }
        return ret;
    }
    auto topicIter = _usingBrokerTopics.begin();
    while (topicIter != _usingBrokerTopics.end()) {
        auto rolesIter = _brokerTopicApplers.find(topicIter->first);
        if (rolesIter == _brokerTopicApplers.end()) {
            if (!topicIter->second->destroyAllBrokerTopic()) {
                ret = false;
                ++topicIter;
            } else {
                topicIter = _usingBrokerTopics.erase(topicIter);
            }
            continue;
        }
        if (rolesIter->second.size() == 0) {
            if (!topicIter->second->destroyAllBrokerTopic()) {
                ret = false;
                ++topicIter;
            } else {
                topicIter = _usingBrokerTopics.erase(topicIter);
                _brokerTopicApplers.erase(rolesIter);
            }
            continue;
        }
        ++topicIter;
    }
    return ret;
}

}} // namespace build_service::common
