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
#include "build_service/admin/FlowIdMaintainer.h"

#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, FlowIdMaintainer);

int64_t FlowIdMaintainer::DEFAULT_CLEAR_INTERVAL = 3 * 60 * 60; // 3h 单位s

FlowIdMaintainer::FlowIdMaintainer()
{
    _clearInterval = DEFAULT_CLEAR_INTERVAL;
    string param = autil::EnvUtil::getEnv(BS_ENV_FLOW_CLEAR_INTERVAL.c_str());
    int64_t tmp = 0;
    if (!param.empty() && StringUtil::fromString(param, tmp) && tmp > 0) {
        _clearInterval = tmp;
    }
    BS_LOG(INFO, "flow clear interval is %ld seconds", _clearInterval);
}

FlowIdMaintainer::~FlowIdMaintainer() {}

void FlowIdMaintainer::cleanFlows(const TaskFlowManagerPtr& manager, const string& generationDir, bool keepRelativeFlow,
                                  bool keepRecentFlow)
{
    int64_t curTs = TimeUtility::currentTimeInSeconds();
    auto allFlows = manager->getAllFlow();

    set<string> keepFlowSet;
    if (keepRelativeFlow) {
        for (auto& flow : allFlows) {
            if (!flow || flow->isFlowFinish() || flow->isFlowStopped()) {
                continue;
            }
            vector<string> flowIds = flow->getUpstreamFlowIds();
            for (auto& id : flowIds) {
                keepFlowSet.insert(id);
            }
            vector<string> friendFlows = flow->getFriendFlowIds();
            for (auto& friendFlow : friendFlows) {
                keepFlowSet.insert(friendFlow);
            }
        }
    }
    if (keepRecentFlow) {
        string recentFlowId;
        auto recentFlowTs = -1;
        for (auto& flow : _stopFlowInfos) {
            if (flow.second > recentFlowTs) {
                recentFlowId = flow.first;
                recentFlowTs = flow.second;
            }
        }
        if (recentFlowTs != -1) {
            keepFlowSet.insert(recentFlowId);
        }
    }

    auto iter = allFlows.begin();
    while (iter != allFlows.end()) {
        if (!(*iter)->isFlowFinish() && !(*iter)->isFlowStopped()) {
            _stopFlowInfos.erase((*iter)->getFlowId());
            iter++;
            continue;
        }
        string flowId = (*iter)->getFlowId();
        auto stopFlowIter = _stopFlowInfos.find(flowId);
        if (stopFlowIter == _stopFlowInfos.end()) {
            _stopFlowInfos.insert(make_pair(flowId, curTs));
            iter++;
            continue;
        }
        if (stopFlowIter->second + _clearInterval < curTs && keepFlowSet.find(flowId) == keepFlowSet.end()) {
            clearWorkerZkNode((*iter), generationDir);
            manager->removeFlow(stopFlowIter->first, true);
            _stopFlowInfos.erase(stopFlowIter);
            BS_LOG(INFO, "clear generationDir[%s] flow[%s]", generationDir.c_str(), flowId.c_str());
        }
        iter++;
    }
}

void FlowIdMaintainer::clearWorkerZkNode(const TaskFlowPtr& flow, const string& generationDir)
{
    FSLIB_LONG_INTERVAL_LOG("clear generation [%s] zk nodes", generationDir.c_str());
    auto tasks = flow->getTasks();
    for (auto& task : tasks) {
        BuildServiceTaskPtr bsTask = DYNAMIC_POINTER_CAST(BuildServiceTask, task);
        if (!bsTask) {
            continue;
        }
        bsTask->clearWorkerZkNode(generationDir);
    }
}

void FlowIdMaintainer::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("stop_flow_infos", _stopFlowInfos, _stopFlowInfos);
}

}} // namespace build_service::admin
