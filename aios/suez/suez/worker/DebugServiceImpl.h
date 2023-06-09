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

#include <memory>

#include "autil/legacy/json.h"
#include "suez/worker/Debug.pb.h"

namespace suez {
class RpcServer;
class TaskExecutor;
class HeartbeatTarget;
class SchedulerInfo;

class DebugServiceImpl final : public DebugService {
public:
    DebugServiceImpl();
    ~DebugServiceImpl();

public:
    bool init(RpcServer *rpcServer, const std::shared_ptr<TaskExecutor> &executor);
    void nextStep(google::protobuf::RpcController *controller,
                  const TargetRequest *request,
                  NextStepResponse *response,
                  google::protobuf::Closure *done) override;

private:
    bool parseFromTarget(const TargetRequest *request,
                         HeartbeatTarget &heartbeatTarget,
                         HeartbeatTarget &finalHeartbeatTarget,
                         SchedulerInfo &schedulerInfo);
    void getGlobalCustomInfo(const std::string &globalCustomInfo, std::string &finalTarget);
    void setResponse(std::pair<bool, autil::legacy::json::JsonMap> &result, NextStepResponse *response);

private:
    std::shared_ptr<TaskExecutor> _taskExecutor;
};

} // namespace suez
