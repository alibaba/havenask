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
#include <string>
#include <vector>

#include "autil/NoCopyable.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/deploy/DeployFiles.h"
#include "suez/deploy/IndexChecker.h"

namespace indexlibv2 {
namespace config {
class TabletOptions;
}
} // namespace indexlibv2

namespace indexlibv2 {
namespace framework {
struct VersionDeployDescription;
}
} // namespace indexlibv2
namespace suez {

class DeployManager;
class FileDeployer;

class IndexDeployer : public autil::NoCopyable {
public:
    struct IndexPathDetail {
        std::string rawIndexRoot;
        std::string indexRoot;
        std::string localIndexRoot;
        std::string checkIndexPath;
        std::string loadConfigPath;
    };

public:
    IndexDeployer(const PartitionId &pid, DeployManager *deployManager);
    virtual ~IndexDeployer();

public:
    virtual DeployStatus deploy(const IndexPathDetail &pathDetail,
                                IncVersion oldVersionId,
                                IncVersion newVersionId,
                                const indexlibv2::config::TabletOptions &baseTabletOptions,
                                const indexlibv2::config::TabletOptions &targetTabletOptions);
    virtual void cancel();

    bool cleanDoneFiles(const std::string &localRootPath, std::function<bool(IncVersion)> removePredicate);
    virtual bool cleanDoneFiles(const std::string &localRootPath, const std::set<IncVersion> &inUseVersions);

    bool getNeedKeepDeployFiles(const std::string &localRootPath,
                                IncVersion keepBeginVersionId,
                                std::set<std::string> *whiteList) const;

    static bool checkDeployDone(const std::string &versionFile,
                                const std::string &doneFile,
                                const indexlibv2::framework::VersionDeployDescription &targetDpDescription);

    static bool markDeployDone(const std::string &doneFile,
                               const indexlibv2::framework::VersionDeployDescription &targetDpDescription);

    static bool loadDeployDone(const std::string &doneFile,
                               indexlibv2::framework::VersionDeployDescription &targetDpDescription);

    static void setIgnoreLocalIndexCheckFlag(bool ignoreLocalIndexCheck);

private:
    bool deployVersionFile(const std::string &rawPartitionPath,
                           const std::string &localPartitionPath,
                           IncVersion oldVersionId,
                           IncVersion newVersionId);

    bool deployExtraFiles(const std::string &rawIndexRoot, const std::string &localRootPath) const;

    bool getDeployFiles(const std::string &remoteIndexRoot,
                        const std::string &localRootPath,
                        const std::string &remotePath,
                        const indexlibv2::config::TabletOptions &baseTabletOptions,
                        const indexlibv2::config::TabletOptions &targetTabletOptions,
                        IncVersion oldVersionId,
                        IncVersion newVersionId,
                        const indexlibv2::framework::VersionDeployDescription &baseVersionDpDesc,
                        DeployFilesVec &localDeployFilesVec,
                        indexlibv2::framework::VersionDeployDescription &targetVersionDpDesc) const;

    static bool listDoneFiles(const std::string &localPartitionRoot,
                              std::vector<std::pair<std::string, IncVersion>> *doneFileList);

    std::string formatMsg(const std::string &msg,
                          IncVersion oldVersionId,
                          IncVersion newVersionId,
                          const std::string &remoteDataPath) const;
    void
    logError(const std::string &remoteDataPath, IncVersion oldVersionId, IncVersion newVersionId, DeployStatus ret);

private:
    static std::vector<std::string> dedupFiles(const std::vector<std::string> &fileNames);
    static std::string getVersionFileName(IncVersion version);
    static bool getVersionLocalDeployFiles(IncVersion version, std::set<std::string> *localDeployedFiles);

    static bool getLocalDeployFilesInVersion(const std::string &doneFilePath,
                                             std::set<std::string> *localDeployedFiles);
    static bool IGNORE_LOCAL_INDEX_CHECK;

private:
    PartitionId _pid;
    IndexChecker _indexChecker;
    std::unique_ptr<FileDeployer> _fileDeployer;
};

} // namespace suez
