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
#include "suez/admin/AdminOps.h"

using namespace std;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, AdminOps);

AdminOps::AdminOps() = default;

AdminOps::~AdminOps() = default;

bool AdminOps::init(const std::string &storeUri,
                    bool localCarbon,
                    bool localCm2Mode,
                    const std::shared_ptr<carbon::RoleManagerWrapper> &roleManager) {
    if (storeUri.empty()) {
        return false;
    }
    _catalogService = std::make_unique<catalog::CatalogServiceImpl>(storeUri);
    _clusterService = std::make_unique<ClusterServiceImpl>(storeUri);
    _schedulerService = std::make_unique<SchedulerServiceImpl>(localCarbon, localCm2Mode, roleManager);

    return true;
}

bool AdminOps::start() {
    if (!_catalogService->start()) {
        AUTIL_LOG(ERROR, "start catalog service failed");
        return false;
    }
    if (!_clusterService->start()) {
        AUTIL_LOG(ERROR, "start catalog service failed");
        return false;
    }
    if (!_schedulerService->start(_catalogService->getManager(), _clusterService->getManager())) {
        AUTIL_LOG(ERROR, "start schedluer service failed");
        return false;
    }
    return true;
}

void AdminOps::stop() {
    _schedulerService->stop();
    _catalogService->stop();
    _clusterService->stop();
}

std::vector<RPCService *> AdminOps::listService() {
    return {_catalogService.get(), _clusterService.get(), _schedulerService.get()};
}

} // namespace suez
