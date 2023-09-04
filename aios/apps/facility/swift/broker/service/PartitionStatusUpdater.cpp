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
#include "swift/broker/service/PartitionStatusUpdater.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/MessageComparator.h"

using namespace std;

using namespace swift::protocol;
namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, PartitionStatusUpdater);

PartitionStatusUpdater::PartitionStatusUpdater(TopicPartitionSupervisor *tpSupervisor) : _tpSupervisor(tpSupervisor) {}

PartitionStatusUpdater::~PartitionStatusUpdater() {}

void PartitionStatusUpdater::updateTaskInfo(const protocol::DispatchInfo &msg) {
    set<TaskInfo> taskInfoSet;
    if (!getTPFromDispatchInfo(msg, taskInfoSet)) {
        return;
    }
    assert(_tpSupervisor);
    vector<PartitionId> currentPartitions;
    _tpSupervisor->getAllPartitions(currentPartitions);

    vector<TaskInfo> toLoad;
    vector<PartitionId> toUnLoad;
    doDiff(currentPartitions, taskInfoSet, toLoad, toUnLoad);
    // do unload command before load command
    setForceUnload(toLoad, toUnLoad);
    _tpSupervisor->unLoadBrokerPartition(toUnLoad);
    _tpSupervisor->loadBrokerPartition(toLoad);
}

void PartitionStatusUpdater::doDiff(const vector<PartitionId> &current,
                                    set<TaskInfo> &target,
                                    vector<TaskInfo> &toLoad,
                                    vector<PartitionId> &toUnLoad) {
    for (vector<PartitionId>::const_iterator it = current.begin(); it != current.end(); ++it) {
        const PartitionId &partId = *it;
        set<TaskInfo>::iterator sIt = target.begin();
        bool existInTarget = false;
        for (; sIt != target.end();) {
            if (sIt->partitionid() == partId) {
                sIt = target.erase(sIt);
                existInTarget = true;
                break;
            } else {
                ++sIt;
            }
        }
        // this partition is not in target. unload it.
        if (!existInTarget) {
            toUnLoad.push_back(partId);
        }
    }

    // these partitions are not in current, load it.
    for (set<TaskInfo>::const_iterator it = target.begin(); it != target.end(); ++it) {
        toLoad.push_back(*it);
    }
}

bool PartitionStatusUpdater::getTPFromDispatchInfo(const DispatchInfo &msg,
                                                   set<protocol::TaskInfo> &taskInfoSet) const {
    int size = msg.taskinfos_size();
    for (int idx = 0; idx < size; ++idx) {
        const protocol::TaskInfo &taskInfo = msg.taskinfos(idx);
        if (taskInfo.has_partitionid() && taskInfo.partitionid().has_id() && taskInfo.partitionid().has_topicname()) {
            taskInfoSet.insert(taskInfo);
        } else {
            AUTIL_LOG(ERROR, "taskInfo in DispatchInfo is Invalid");
            return false;
        }
    }
    return true;
}

void PartitionStatusUpdater::setForceUnload(vector<TaskInfo> &toLoad, vector<PartitionId> &toUnLoad) {
    for (size_t i = 0; i < toUnLoad.size(); i++) {
        for (size_t j = 0; j < toLoad.size(); j++) {
            if (toLoad[j].partitionid().topicname() == toUnLoad[i].topicname() &&
                toLoad[j].partitionid().id() == toUnLoad[i].id() && toLoad[j].topicmode() == TOPIC_MODE_MEMORY_ONLY) {
                toUnLoad[i].set_forceunload(true);
                break;
            }
        }
    }
}
} // namespace service
} // namespace swift
