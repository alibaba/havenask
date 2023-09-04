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

#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "autil/NoCopyable.h"
#include "suez/deploy/DeployFiles.h"
#include "suez/deploy/DeployItem.h"
#include "worker_framework/DataOption.h"

namespace worker_framework {
namespace data_client {
class DataClient;
}
} // namespace worker_framework

namespace suez {

class DiskQuotaController;
class DeployItem;

class DeployManager : public autil::NoCopyable {
public:
    DeployManager(const std::shared_ptr<worker_framework::DataClient> &dataClient,
                  DiskQuotaController *diskQuotaController,
                  bool localMode = false);
    virtual ~DeployManager();

public:
    // virtual for test
    virtual std::shared_ptr<DeployItem> deploy(const DeployFilesVec &deployFilesVec);
    void updateDeployConfig(const worker_framework::DataOption &dataOption);

    void erase(const DeployFilesVec &deployFiles);

private:
    DeployResource _deployResource;
    bool _localMode;
    mutable std::mutex _mu;
    std::unordered_map<DeployFilesVec, std::shared_ptr<DeployItem>> _deployFilesSet;
    worker_framework::DataOption _dataOption;
};

} // namespace suez
