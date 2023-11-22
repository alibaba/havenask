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
#include "build_service/admin/taskcontroller/DocReclaimTaskController.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/config/AgentGroupConfig.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, DocReclaimTaskController);

DocReclaimTaskController::DocReclaimTaskController(const string& taskId, const string& taskName,
                                                   const TaskResourceManagerPtr& resMgr)
    : DefaultTaskController(taskId, taskName, resMgr)
    , _stopTs(-1)
{
}

DocReclaimTaskController::~DocReclaimTaskController() {}

bool DocReclaimTaskController::doInit(const string& clusterName, const string& taskConfigPath, const string& initParam)
{
    _partitionCount = 1;
    _parallelNum = 1;
    return true;
}

bool DocReclaimTaskController::start(const KeyValueMap& kvMap)
{
    string mergeConfigName;
    auto iter = kvMap.find("mergeConfigName");
    if (iter == kvMap.end()) {
        BS_LOG(ERROR, "cannot get mergeConfigName from param for doc reclaim task");
        return false;
    }
    mergeConfigName = iter->second;
    DocReclaimSource source;
    if (!prepareDocReclaimTask(mergeConfigName, source)) {
        // no need to do reclaim task
        return true;
    }

    _targets.clear();
    _stopTs = TimeUtility::currentTimeInSeconds();
    TaskTarget target(DEFAULT_TARGET_NAME);
    target.setPartitionCount(_partitionCount);
    target.setParallelNum(_parallelNum);
    target.addTargetDescription("stopTime", StringUtil::toString(_stopTs));
    target.addTargetDescription("reclaimSource", ToJsonString(source, true));
    _targets.push_back(target);
    return true;
}

bool DocReclaimTaskController::prepareDocReclaimTask(const string& mergeConfigName, DocReclaimSource& source)
{
    if (mergeConfigName.empty()) {
        return false;
    }
    ConfigReaderAccessorPtr readerAccessor;
    _resourceManager->getResource(readerAccessor);
    ResourceReaderPtr resourceReader = readerAccessor->getLatestConfig();

    bool isExist = false;
    string jsonPath =
        "offline_index_config.customized_merge_config." + mergeConfigName + "." + DocReclaimSource::DOC_RECLAIM_SOURCE;
    if (!resourceReader->getClusterConfigWithJsonPath(_clusterName, jsonPath, source, isExist) || !isExist) {
        BS_LOG(WARN, "no doc_reclaim_source is configurated for %s, no need to run DocReclaimTask",
               _clusterName.c_str());
        return false;
    }

    return true;
}

void DocReclaimTaskController::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    DefaultTaskController::Jsonize(json);
    json.Jsonize("stop_ts", _stopTs, _stopTs);
}

}} // namespace build_service::admin
