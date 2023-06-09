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

#include <vector>

#include "suez/deploy/DeployItem.h"

namespace worker_framework {
class DataClient;
class DataItem;
} // namespace worker_framework

namespace suez {

class NormalDeployItem : public DeployItem {
public:
    NormalDeployItem(const DeployResource &deployResource,
                     const worker_framework::DataOption &dataOption,
                     const DeployFilesVec &deployFilesVec);
    ~NormalDeployItem() = default;

public:
    DeployStatus deployAndWaitDone() override;
    void cancel() override;

private:
    DeployStatus deploy(std::vector<std::shared_ptr<worker_framework::DataItem>> &dataItems);
    DeployStatus doDeploy(std::vector<std::shared_ptr<worker_framework::DataItem>> &dataItems) const;
    DeployStatus waitDone(const std::vector<std::shared_ptr<worker_framework::DataItem>> &dataItems) const;
    DeployStatus doDeployFilesAsync(const std::string &remoteDataPath,
                                    const std::string &localPath,
                                    DeployFiles deployFiles,
                                    const worker_framework::DataOption &dataOption,
                                    std::shared_ptr<worker_framework::DataItem> &dataItem) const;

private:
    const worker_framework::DataOption _dataOption;
    std::vector<std::shared_ptr<worker_framework::DataItem>> _dataItems;
};

} // namespace suez
