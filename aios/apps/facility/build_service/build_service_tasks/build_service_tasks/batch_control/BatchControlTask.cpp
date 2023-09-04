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
#include "build_service_tasks/batch_control/BatchControlTask.h"

#include <beeper/beeper.h>

#include "autil/HashFuncFactory.h"
#include "autil/StringUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/SwiftClientCreator.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace autil;
using namespace build_service::util;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::common;
using namespace std;

using namespace swift::client;
using namespace swift::protocol;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, BatchControlTask);

const string BatchControlTask::TASK_NAME = BS_TASK_BATCH_CONTROL;

BatchControlTask::BatchControlTask() : _hasStarted(false), _client(NULL), _swiftReader(NULL), _port(-1) {}

BatchControlTask::~BatchControlTask()
{
    DELETE_AND_SET_NULL(_swiftReader);
    DELETE_AND_SET_NULL(_client);
}

bool BatchControlTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    _worker.reset(new BatchControlWorker);
    if (!_worker->init(initParam, _beeperTags)) {
        BS_LOG(ERROR, "init BatchControlWorker failed.");
        return false;
    }
    return true;
}

bool BatchControlTask::handleTarget(const config::TaskTarget& target)
{
    string address;
    if (!target.getTargetDescription(BS_TASK_BATCH_CONTROL_ADDRESS, address)) {
        BS_LOG(ERROR, "lack of adminAddress");
        return false;
    }
    int32_t port = -1;
    if (!target.getTargetDescription(BS_TASK_BATCH_CONTROL_PORT, port)) {
        BS_LOG(ERROR, "lack of port, or invalid port");
        return false;
    }

    bool useV2Graph = false;
    string useV2GraphStr;
    if (target.getTargetDescription("useV2Graph", useV2GraphStr) && useV2GraphStr == "true") {
        useV2Graph = true;
    }

    if (_hasStarted) {
        _worker->setUseV2Graph(useV2Graph);
        // if ip or port is change, just reset worker host
        if (_adminAddress != address || _port != port) {
            if (!_worker->resetHost(address, port)) {
                return false;
            }
            _adminAddress = address;
            _port = port;
            BS_LOG(INFO, "reset address [%s], port [%d]", _adminAddress.c_str(), _port);
        }
        return true;
    }
    _adminAddress = address;
    _port = port;
    string topicName;
    if (!target.getTargetDescription(BS_TASK_BATCH_CONTROL_TOPIC, topicName) || topicName.empty()) {
        BS_LOG(ERROR, "lack of topic name");
        return false;
    }
    _dataDesc[SWIFT_TOPIC_NAME] = topicName;
    string swiftRoot;
    if (!target.getTargetDescription(BS_TASK_BATCH_CONTROL_SWIFT_ROOT, swiftRoot) || swiftRoot.empty()) {
        BS_LOG(ERROR, "lack of swift root");
        return false;
    }
    _dataDesc[SWIFT_ZOOKEEPER_ROOT] = swiftRoot;
    int64_t startTs = 0;
    if (!target.getTargetDescription(BS_TASK_BATCH_CONTROL_START_TS, startTs)) {
        BS_LOG(INFO, "no startTimestamp, use 0");
    }
    _dataDesc[SWIFT_START_TIMESTAMP] = StringUtil::toString(startTs);
    // for compatity, dont remove params mentioned above
    // for other params not mentioned above
    const KeyValueMap& ds = target.getTargetDescription();
    KeyValueMap::const_iterator dsIter = ds.begin();
    while (dsIter != ds.end()) {
        if (dsIter->first == BS_TASK_BATCH_CONTROL_TOPIC || dsIter->first == BS_TASK_BATCH_CONTROL_SWIFT_ROOT ||
            dsIter->first == BS_TASK_BATCH_CONTROL_START_TS) {
            dsIter++;
            continue;
        }
        _dataDesc[dsIter->first] = dsIter->second;
        dsIter++;
    }

    _worker->setUseV2Graph(useV2Graph);
    _worker->setDataDescription(_dataDesc);
    if (!initSwiftReader(topicName, swiftRoot)) {
        return false;
    }
    if (!_worker->start(_swiftReader, startTs, _adminAddress, _port)) {
        return false;
    }

    _hasStarted = true;
    return true;
}

bool BatchControlTask::initSwiftReader(const string& topicName, const string& swiftRoot)
{
    // init swift client
    _client = new SwiftClient();
    string configStr = string(swift::client::CLIENT_CONFIG_ZK_PATH) + "=" + swiftRoot;
    swift::protocol::ErrorCode errorCode = _client->initByConfigStr(configStr);
    if (errorCode != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "create swift client failed, ec[%d]", errorCode);
        return false;
    }

    stringstream ss;
    ss << "topicName=" << topicName << ";"
       << "from=" << 0 << ";"
       << "to=" << 65535 << ";"
       << "uint8FilterMask=" << (uint32_t)255 << ";"
       << "uint8MaskResult=" << (uint32_t)0;

    swift::protocol::ErrorInfo errorInfo;
    _swiftReader = _client->createReader(ss.str(), &errorInfo);
    if (!_swiftReader) {
        BS_LOG(ERROR, "create swift reader failed.");
        return false;
    }
    if (errorInfo.errcode() != swift::protocol::ERROR_NONE) {
        BS_LOG(ERROR, "create swift reader failed, error msg[%s]", errorInfo.errmsg().c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::task_base
