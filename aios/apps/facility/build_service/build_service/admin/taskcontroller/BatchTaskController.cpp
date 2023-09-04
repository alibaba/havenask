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
#include "build_service/admin/taskcontroller/BatchTaskController.h"

#include "autil/StringUtil.h"
#include "build_service/admin/AdminInfo.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/BrokerTopicAccessor.h"
#include "build_service/common/SwiftAdminFacade.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/util/DataSourceHelper.h"

using namespace std;
using namespace autil::legacy;
using namespace autil;
using namespace build_service::config;
using namespace build_service::common;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, BatchTaskController);

BatchTaskController::~BatchTaskController() { deregisterTopic(); }

bool BatchTaskController::doInit(const string& clusterName, const string& taskConfigFilePath, const string& initParam)
{
    _partitionCount = 1;
    _parallelNum = 1;
    map<string, string> taskParam;
    try {
        FromJsonString(taskParam, initParam);
        auto iter = taskParam.find("clusters");
        if (iter == taskParam.end()) {
            BS_LOG(ERROR, "lack of clusters");
            return false;
        }
        FromJsonString(_clusters, iter->second);
    } catch (const autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "INVALID json str[%s]", initParam.c_str());
        return false;
    }

    if (!registerTopic()) {
        return false;
    }
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);
    if (!prepareTargetDescrition(taskParam, initParam, target)) {
        return false;
    }
    _targets.push_back(target);
    return true;
}

bool BatchTaskController::registerTopic()
{
    BrokerTopicAccessorPtr topicAccessor;
    _resourceManager->getResource(topicAccessor);
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getLatestConfig();
    assert(reader);
    for (auto cluster : _clusters) {
        string topicName, swiftRoot;
        tie(topicName, swiftRoot) = topicAccessor->registBrokerTopic(_taskId, reader, cluster, BS_TOPIC_INC, "");
        if (topicName.empty()) {
            return false;
        }
        BS_LOG(DEBUG, "[%s] regist inc broker topic [%s]", _taskId.c_str(), cluster.c_str());
    }
    return true;
}

bool BatchTaskController::operate(TaskController::Nodes& taskNodes)
{
    registerTopic();
    return DefaultTaskController::operate(taskNodes);
}

bool BatchTaskController::deregisterTopic()
{
    BrokerTopicAccessorPtr topicAccessor;
    _resourceManager->getResource(topicAccessor);
    config::ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    auto reader = readerAccessor->getLatestConfig();
    if (!reader) {
        BS_LOG(ERROR, "deregister topic failed");
        return false;
    }
    for (auto cluster : _clusters) {
        string topicName = topicAccessor->getTopicName(reader, cluster, BS_TOPIC_INC, "");
        topicAccessor->deregistBrokerTopic(_taskId, topicName);
        BS_LOG(DEBUG, "[%s] regist inc broker topic [%s]", _taskId.c_str(), topicName.c_str());
    }
    return true;
}

void BatchTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("clusters", _clusters, _clusters);
    if (json.GetMode() == FROM_JSON) {
        AdminInfo* adminInfo = AdminInfo::GetInstance();
        string newAddress = adminInfo->getAdminAddress();
        uint32_t newPort = adminInfo->getAdminHttpPort();
        string address;
        if (_targets[0].getTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, address)) {
            _targets[0].updateTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, newAddress);
        } else {
            _targets[0].addTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, newAddress);
        }
        string port;
        if (_targets[0].getTargetDescription(BS_TASK_BATCH_CONTROL_PORT, port)) {
            _targets[0].updateTargetDescription(BS_TASK_BATCH_CONTROL_PORT, StringUtil::toString(newPort));
        } else {
            _targets[0].addTargetDescription(BS_TASK_BATCH_CONTROL_PORT, StringUtil::toString(newPort));
        }
        BS_LOG(INFO, "set admin address [%s], port [%u]", newAddress.c_str(), newPort);
        registerTopic();
    }
}

bool BatchTaskController::prepareTargetDescrition(const map<string, string>& taskParam, const string& initParam,
                                                  TaskTarget& taskTarget)
{
    proto::DataDescriptions dataDescriptions;
    try {
        auto iter = taskParam.find("dataDescriptions");
        if (iter == taskParam.end()) {
            BS_LOG(ERROR, "lack of dataDescriptions");
            return false;
        }
        FromJsonString(dataDescriptions.toVector(), iter->second);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "parse data descriptions from [" << initParam << "] failed, exception[" << string(e.what()) << "]";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    size_t dataDescriptionSize = dataDescriptions.size();
    if (dataDescriptionSize <= 0) {
        BS_LOG(ERROR, "data descriptions size is [%u], smaller than 0, init param is [%s]",
               (unsigned int)dataDescriptionSize, initParam.c_str());
        return false;
    }
    proto::DataDescription lastDs = dataDescriptions[dataDescriptionSize - 1];
    if (!util::DataSourceHelper::isRealtime(lastDs)) {
        BS_LOG(ERROR, "last data description is not swift-like [%s]", initParam.c_str());
        return false;
    }

    auto iter = lastDs.find(SRC_BATCH_MODE);
    if (iter == lastDs.end() || iter->second != "true") {
        BS_LOG(ERROR, "last data description is not batch mode[%s]", initParam.c_str());
        return false;
    }
    iter = lastDs.find(SWIFT_ZOOKEEPER_ROOT);
    if (iter == lastDs.end()) {
        BS_LOG(ERROR, "invalid data description, not swift zk root [%s]", initParam.c_str());
        return false;
    }
    taskTarget.addTargetDescription(BS_TASK_BATCH_CONTROL_SWIFT_ROOT, iter->second);
    BS_LOG(INFO, "set batch controller swift root [%s]", iter->second.c_str());
    iter = lastDs.find(SWIFT_TOPIC_NAME);
    if (iter == lastDs.end()) {
        BS_LOG(ERROR, "invalid data description, not swift topic [%s]", initParam.c_str());
        return false;
    }
    taskTarget.addTargetDescription(BS_TASK_BATCH_CONTROL_TOPIC, iter->second);
    BS_LOG(INFO, "set batch controller topic name [%s]", iter->second.c_str());
    iter = lastDs.find(SWIFT_START_TIMESTAMP);
    if (iter != lastDs.end()) {
        taskTarget.addTargetDescription(BS_TASK_BATCH_CONTROL_START_TS, iter->second);
        BS_LOG(INFO, "set batch controller start at [%s]", iter->second.c_str());
    }
    // for compatity, dont remove params mentioned above
    // for other params not mentioned above
    KeyValueMap::iterator dsIter = lastDs.begin();
    while (dsIter != lastDs.end()) {
        if (dsIter->first == SWIFT_START_TIMESTAMP || dsIter->first == SWIFT_TOPIC_NAME ||
            dsIter->first == SWIFT_ZOOKEEPER_ROOT) {
            dsIter++;
            continue;
        }
        taskTarget.addTargetDescription(dsIter->first, dsIter->second);
        dsIter++;
    }

    auto it = taskParam.find("useV2Graph");
    if (it != taskParam.end() && it->second == "true") {
        BS_LOG(INFO, "use v2 graph for batchTaskController.");
        taskTarget.addTargetDescription("useV2Graph", "true");
    }

    AdminInfo* adminInfo = AdminInfo::GetInstance();
    string address = adminInfo->getAdminAddress();
    string port = StringUtil::toString(adminInfo->getAdminHttpPort());
    taskTarget.addTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, address);
    taskTarget.addTargetDescription(BS_TASK_BATCH_CONTROL_PORT, port);
    BS_LOG(INFO, "set batch controller admin address [%s], port [%s]", address.c_str(), port.c_str());
    return true;
}

bool BatchTaskController::start(const KeyValueMap& kvMap) { return true; }

bool BatchTaskController::updateConfig() { return true; }

}} // namespace build_service::admin
