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
#include <mutex>
#include <string>
#include <vector>

#include "autil/NoCopyable.h"
#include "suez/deploy/DeployFiles.h"
#include "suez/table/InnerDef.h"

namespace worker_framework {
class DataClient;
} // namespace worker_framework

namespace suez {

class DiskQuotaController;

struct DeployResource {
    std::shared_ptr<worker_framework::DataClient> dataClient;
    DiskQuotaController *diskQuotaController;
};

class DeployItem : public autil::NoCopyable {
public:
    DeployItem(const DeployResource &deployResource, const DeployFilesVec &deployFilesVec);
    virtual ~DeployItem() = default;

public:
    virtual DeployStatus deployAndWaitDone() = 0;
    virtual void cancel() = 0;

protected:
    DeployStatus reserveDiskQuota(const std::string &remoteDataPath,
                                  const std::string &localDataPath,
                                  DeployFiles &deployFiles) const;

    static std::string deployFilesSummary(const DeployFiles &deployFiles);
    static bool checkAndFillDeployFiles(DeployFiles &deployFiles);
    static bool getFilesAndSize(const std::string &remotePath, std::vector<std::string> &files, int64_t &size);
    static bool getFilesSize(const std::string &remotePath, const std::vector<std::string> &files, int64_t &size);

protected:
    const DeployResource _deployResource;
    DeployFilesVec _deployFilesVec;
    mutable std::mutex _mu;
};

} // namespace suez
