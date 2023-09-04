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

#include "aios/network/arpc/arpc/ANetRPCServer.h"
#include "carbon/RoleManagerWrapper.h"
#include "catalog/service/CatalogServiceImpl.h"
#include "suez/admin/ClusterServiceImpl.h"
#include "suez/admin/SchedulerServiceImpl.h"

namespace suez {

class AdminOps {
public:
    AdminOps();
    ~AdminOps();

public:
    bool init(const std::string &storeUri,
              bool localCarbon,
              bool localCm2Mode,
              const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager);

    bool start();
    void stop();
    std::vector<RPCService *> listService();

private:
    std::unique_ptr<catalog::CatalogServiceImpl> _catalogService;
    std::unique_ptr<ClusterServiceImpl> _clusterService;
    std::unique_ptr<SchedulerServiceImpl> _schedulerService;
};

} // namespace suez
