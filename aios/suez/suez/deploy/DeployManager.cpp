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
#include "suez/deploy/DeployManager.h"

#include "suez/deploy/LocalDeployItem.h"
#include "suez/deploy/NormalDeployItem.h"

using namespace std;

namespace suez {

DeployManager::DeployManager(const std::shared_ptr<worker_framework::DataClient> &dataClient,
                             DiskQuotaController *diskQuotaController,
                             bool localMode)
    : _deployResource{dataClient, diskQuotaController}, _localMode(localMode) {}

DeployManager::~DeployManager() {}

shared_ptr<DeployItem> DeployManager::deploy(const DeployFilesVec &deployFilesVec) {
    unique_lock<mutex> lock(_mu);
    auto it = _deployFilesSet.find(deployFilesVec);
    if (it != _deployFilesSet.end()) {
        return it->second;
    }

    shared_ptr<DeployItem> item;
    if (!_localMode) {
        item = make_shared<NormalDeployItem>(_deployResource, _dataOption, deployFilesVec);
    } else {
        item = make_shared<LocalDeployItem>(_deployResource, deployFilesVec);
    }

    _deployFilesSet[deployFilesVec] = item;
    return item;
}

void DeployManager::updateDeployConfig(const worker_framework::DataOption &dataOption) {
    unique_lock<mutex> lock(_mu);
    _dataOption = dataOption;
}

void DeployManager::erase(const DeployFilesVec &deployFiles) {
    unique_lock<mutex> lock(_mu);
    _deployFilesSet.erase(deployFiles);
}

} // namespace suez
