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
#include "build_service/admin/AlterFieldCKPAccessor.h"

#include "autil/StringUtil.h"
#include "build_service/admin/taskcontroller/AlterFieldTask.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

using build_service::common::CheckpointAccessorPtr;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, AlterFieldCKPAccessor);

AlterFieldCKPAccessor::AlterFieldCKPAccessor() {}

AlterFieldCKPAccessor::~AlterFieldCKPAccessor() {}

void AlterFieldCKPAccessor::updateCheckpoint(const std::string& clusterName, schema_opid_t opsId,
                                             CheckpointAccessorPtr checkpointAccessor)
{
    checkpointAccessor->addCheckpoint(AlterFieldTask::getAlterFieldCheckpointId(clusterName),
                                      StringUtil::toString(opsId), "");
}

bool AlterFieldCKPAccessor::getOngoingOpIds(const CheckpointAccessorPtr& checkpointAccessor, const string& clusterName,
                                            schema_opid_t maxOpId, vector<schema_opid_t>& ongoingOpIds)
{
    vector<pair<string, string>> checkpoints;
    checkpointAccessor->listCheckpoints(AlterFieldTask::getAlterFieldCheckpointId(clusterName), checkpoints);

    set<uint32_t> finishedOpIds;
    for (auto iter = checkpoints.begin(); iter != checkpoints.end(); iter++) {
        uint32_t opId = 0;
        if (!StringUtil::fromString(iter->first, opId)) {
            BS_LOG(ERROR, "error opId[%s] for %s", iter->first.c_str(), clusterName.c_str());
            return false;
        }
        if (opId > maxOpId) {
            BS_LOG(WARN, "error opId[%s] for %s, maxOpId[%u]", iter->first.c_str(), clusterName.c_str(), maxOpId);
            continue;
        }
        finishedOpIds.insert(opId);
    }

    for (uint32_t i = 1; i <= maxOpId; i++) {
        if (finishedOpIds.find(i) != finishedOpIds.end()) {
            continue;
        }
        ongoingOpIds.push_back(i);
    }
    return true;
}

}} // namespace build_service::admin
