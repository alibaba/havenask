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

#include "suez/deploy/DeployFiles.h"
#include "suez/table/InnerDef.h"

namespace suez {

class DeployManager;
class DeployItem;

class FileDeployer {
public:
    FileDeployer(DeployManager *deployManager);
    virtual ~FileDeployer();

private:
    FileDeployer(const FileDeployer &);
    FileDeployer &operator=(const FileDeployer &);

public:
    DeployStatus deploy(const std::string &remoteDataPath, const std::string &localPath);

    // virtual for test
    virtual DeployStatus deploy(const DeployFilesVec &deployFilesVec,
                                const std::function<bool()> &checkDeployDoneFunc,
                                const std::function<bool()> &markDeployDoneFunc);
    // virtual for test
    virtual void cancel();

protected:
    // virtual for test
    virtual DeployStatus doDeploy(const DeployFilesVec &deployFilesVec);

public:
    static bool checkDeployDone(const std::string &doneFile, const std::string &rawPath);

    static bool markDeployDone(const std::string &doneFile, const std::string &rawPath);

    static bool cleanDeployDone(const std::string &doneFile);

private:
    DeployManager *_deployManager;
    mutable std::mutex _mu;
    std::shared_ptr<DeployItem> _deployItem;
    static const std::string MARK_FILE_NAME;
    static bool IGNORE_LOCAL_FILE_CHECK;
};

} // namespace suez
