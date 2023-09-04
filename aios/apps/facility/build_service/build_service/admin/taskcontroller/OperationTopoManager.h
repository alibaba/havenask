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

#include <string>
#include <string_view>

#include "build_service/common_define.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/index_task/BasicDefs.h"
namespace build_service::admin {

class OperationTopoManager
{
public:
    struct OpDef {
        proto::OperationDescription desc;
        proto::OperationStatus status = proto::OP_PENDING;
        int64_t assignedNodeId = -1;
        std::string finishedWorkerEpochId;
        std::vector<int64_t> fanouts;
        size_t readyDepends = 0;
    };

    using OpMap = std::map<int64_t, std::shared_ptr<OpDef>>;

public:
    OperationTopoManager() = default;
    ~OperationTopoManager() = default;

public:
    bool init(const proto::OperationPlan& newplan);

    void run(int64_t opId, uint32_t nodeId);
    void finish(int64_t opId, std::string workerEpochId, std::string resultInfo);
    bool isFinished(int64_t opId) const;
    void cancelRunningOps();
    const std::map<int64_t, proto::OperationDescription>& getCurrentExecutableOperations() const;

    size_t finishedOpCount() const { return _finishedOpCount; }
    size_t totalOpCount() const { return _opMap.size(); }
    std::string getTaskResultInfo() const { return _taskResultInfo; }

    auto begin() { return _opMap.begin(); }
    auto end() { return _opMap.end(); }

private:
    bool checkPlan(const OpMap& opMap) const;

private:
    OpMap _opMap;
    size_t _finishedOpCount = 0;
    std::map<int64_t, proto::OperationDescription> _executableOperations;
    int64_t _endTaskOpId;
    std::string _taskResultInfo;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::admin
