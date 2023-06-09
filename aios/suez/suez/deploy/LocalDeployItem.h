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

#include <atomic>

#include "suez/deploy/DeployItem.h"

namespace suez {

class LocalDeployItem : public DeployItem {
public:
    LocalDeployItem(const DeployResource &deployResource, const DeployFilesVec &deployFilesVec);
    ~LocalDeployItem() = default;

public:
    DeployStatus deployAndWaitDone() override;
    void cancel() override;

private:
    DeployStatus
    doDeploy(const std::string &remoteDataPath, const std::string &localDataPath, DeployFiles &deployFiles) const;

private:
    DeployStatus _status = DS_UNKNOWN;
    std::atomic_bool _needCancel = false;
};

} // namespace suez
