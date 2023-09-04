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
#include "build_service/admin/DefaultBrokerTopicCreator.h"

#include "build_service/admin/controlflow/OpenApiHandler.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/config/CLIOptionNames.h"

using namespace std;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, DefaultBrokerTopicCreator);

DefaultBrokerTopicCreator::DefaultBrokerTopicCreator() {}

DefaultBrokerTopicCreator::~DefaultBrokerTopicCreator() {}

std::string DefaultBrokerTopicCreator::getDefaultTopicResourceName(const std::string& clusterName,
                                                                   const std::string& topicConfig,
                                                                   const std::string& topicId)
{
    string resourceName = string("default_topic_") + clusterName + "_" + topicConfig + "_" + topicId;
    return resourceName;
}

common::ResourceKeeperGuardPtr DefaultBrokerTopicCreator::applyDefaultSwiftResource(
    const std::string& roleId, const std::string& clusterName, const proto::BuildStep& step,
    const config::ResourceReaderPtr& configReader, TaskResourceManagerPtr& resourceManager)
{
    common::BrokerTopicAccessorPtr topicAccessor;
    resourceManager->getResource(topicAccessor);
    assert(topicAccessor);
    string topicConfig = step == proto::BUILD_STEP_FULL ? config::BS_TOPIC_FULL : config::BS_TOPIC_INC;
    string topicId = topicAccessor->getTopicId(configReader, clusterName, topicConfig, "");
    string resourceName = getDefaultTopicResourceName(clusterName, topicConfig, topicId);
    KeyValueMap kvMap;
    kvMap["topicConfigName"] = topicConfig;
    kvMap["tag"] = "";
    kvMap["name"] = resourceName;
    kvMap["clusterName"] = clusterName;
    OpenApiHandler openApi(resourceManager);
    if (!openApi.handle("book", "/resource/swift", kvMap)) {
        BS_LOG(ERROR, "prepare broker topic for [%s] failed", clusterName.c_str());
        return common::ResourceKeeperGuardPtr();
    }
    auto topicKeeper = resourceManager->applyResource(roleId, resourceName, configReader);
    if (!topicKeeper) {
        BS_LOG(ERROR, "prepare broker topic for [%s] failed", clusterName.c_str());
    }
    return topicKeeper;
}

}} // namespace build_service::admin
