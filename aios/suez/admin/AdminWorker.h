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
#include <string>

#include "autil/LoopThread.h"
#include "worker_framework/LeaderedWorkerBase.h"

namespace carbon {
class RoleManagerWrapper;
}

namespace suez {

class AdminOps;
class AdminServiceImpl;

class AdminWorker : public worker_framework::LeaderedWorkerBase {
public:
    AdminWorker();
    ~AdminWorker();

private:
    AdminWorker(const AdminWorker &);
    AdminWorker &operator=(const AdminWorker &);

public:
    void doInitOptions() override;
    bool doInit() override;
    bool doStart() override;
    void doStop() override;

private:
    void initEnvArgs();
    void becomeLeader() override{};
    void noLongerLeader() override;

    bool checkIsNewStart(bool &isNewStart);
    bool deleteNewStartTag();

private:
    static const size_t RPC_SERVICE_THREAD_NUM;
    static const size_t RPC_SERVICE_QUEUE_SIZE;
    static const std::string RPC_SERVICE_THEAD_POOL_NAME;
    static const uint32_t ADMIN_DEFAULT_AMONITOR_AGENT_PORT;

private:
    std::string _appId;
    std::string _hippoZk;
    std::string _zkPath;
    int32_t _leaseInSecond;
    int32_t _leaseInLoopInterval;
    uint32_t _amonitorPort;
    uint32_t _arpcPort;
    uint32_t _httpPort;
    bool _opsMode;
    std::string _storeUri;
    bool _localCarbon;
    bool _localCm2Mode;

    std::shared_ptr<carbon::RoleManagerWrapper> _roleManager;
    std::unique_ptr<AdminServiceImpl> _adminService;
    std::unique_ptr<AdminOps> _adminOps;
};

} // namespace suez
