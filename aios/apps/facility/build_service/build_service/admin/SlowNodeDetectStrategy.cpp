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
#include "build_service/admin/SlowNodeDetectStrategy.h"

#include <unordered_set>
#include <utility>

#include "autil/legacy/legacy_jsonizable.h"

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, SlowNodeDetectStrategy);

SlowNodeDetectStrategy::SlowNodeDetectStrategy(const config::SlowNodeDetectConfig& config, int64_t startTime)
    : _slowTimeThreshold(config.slowTimeThreshold * 1000000)
    , _notWorkTimeThreshold(config.notWorkTimeThreshold * 1000000)
    , _restartCountThreshold(config.restartCountThreshold)
    , _startTime(startTime)
    , _excludeKeywords(config.excludeKeywords)
{
    BS_LOG(INFO,
           "initial parameters: slowTimeThreshold[%ld], "
           "restartCountThreshold[%ld], _excludeKeyWords[%s]",
           _slowTimeThreshold, _restartCountThreshold, autil::legacy::ToJsonString(_excludeKeywords).c_str());
}

std::vector<std::string> SlowNodeDetectStrategy::GetExcludeKeyWords(const std::string& strategy) const
{
    auto iter = _excludeKeywords.find(strategy);
    if (iter != _excludeKeywords.end()) {
        return iter->second;
    }
    return {};
}

void SlowNodeDetectStrategy::UniqueNodes(AbnormalNodes* abnormalNodes) const
{
    // reclaim nodes must be handle as soon as possible
    // if there is overlap between reclaimNodes and other (deadNodes, restartNodes, slowNodes)
    // remove them from other (not remove them from reclaimNodes)
    std::unordered_set<taskcontroller::NodeStatusManager::NodeStatusPtr> reclaimSet;
    for (const auto& node : abnormalNodes->reclaimNodes) {
        reclaimSet.insert(node);
    }
    auto InnerUniq = [&reclaimSet](taskcontroller::NodeStatusManager::NodeStatusPtrVec& nodeStatusVec) {
        taskcontroller::NodeStatusManager::NodeStatusPtrVec temp;
        for (const auto& nodeStatus : nodeStatusVec) {
            if (reclaimSet.find(nodeStatus) == reclaimSet.end()) {
                temp.emplace_back(nodeStatus);
            }
        }
        swap(nodeStatusVec, temp);
    };
    InnerUniq(abnormalNodes->deadNodes);
    InnerUniq(abnormalNodes->restartNodes);
    InnerUniq(abnormalNodes->slowNodes);
}

}} // namespace build_service::admin
