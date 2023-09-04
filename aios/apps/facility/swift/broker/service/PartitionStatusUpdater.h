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
#pragma once

#include <set>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/heartbeat/StatusUpdater.h"

namespace swift {
namespace protocol {
class DispatchInfo;
class PartitionId;
class TaskInfo;
} // namespace protocol
namespace service {
class TopicPartitionSupervisor;
} // namespace service
} // namespace swift

namespace swift {
namespace service {

class PartitionStatusUpdater : public heartbeat::StatusUpdater {
public:
    PartitionStatusUpdater(TopicPartitionSupervisor *tpSupervisor);
    ~PartitionStatusUpdater();

private:
    PartitionStatusUpdater(const PartitionStatusUpdater &);
    PartitionStatusUpdater &operator=(const PartitionStatusUpdater &);

public:
    void updateTaskInfo(const protocol::DispatchInfo &msg) override;

private:
    void doDiff(const std::vector<protocol::PartitionId> &current,
                std::set<protocol::TaskInfo> &target,
                std::vector<protocol::TaskInfo> &toLoad,
                std::vector<protocol::PartitionId> &toUnLoad);
    bool getTPFromDispatchInfo(const protocol::DispatchInfo &msg, std::set<protocol::TaskInfo> &taskInfoSet) const;
    void setForceUnload(std::vector<protocol::TaskInfo> &toLoad, std::vector<protocol::PartitionId> &toUnLoad);

private:
    TopicPartitionSupervisor *_tpSupervisor;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(PartitionStatusUpdater);

} // namespace service
} // namespace swift
