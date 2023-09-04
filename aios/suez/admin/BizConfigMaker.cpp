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
#include "suez/admin/BizConfigMaker.h"

#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/md5.h"
#include "autil/result/Errors.h"
#include "catalog/entity/Table.h"
#include "catalog/tools/ConfigFileUtil.h"
#include "fslib/fs/FileSystem.h"
#include "suez/sdk/PathDefine.h"

using namespace std;
using namespace autil::result;
using namespace catalog;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, BizConfigMaker);

autil::Result<std::string> BizConfigMaker::genSignature(const ClusterDeployment &deployment,
                                                        const std::string &templatePath,
                                                        const std::string &storeRoot) {
    std::string tmp;
    deployment.SerializeToString(&tmp);

    std::string content;
    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "analyzer.json"), &content))) {
        return RuntimeError::make("read analyzer file failed");
    }
    tmp += content;

    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "sql.json"), &content))) {
        return RuntimeError::make("read sql file failed");
    }
    tmp += content;

    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "navi.py"), &content))) {
        return RuntimeError::make("read navi file failed");
    }
    tmp += content;
    tmp += storeRoot;
    autil::legacy::Md5Stream stream;
    stream.Put((const uint8_t *)tmp.c_str(), tmp.length());
    return stream.GetMd5String();
}

autil::Result<std::string> BizConfigMaker::make(const ClusterDeployment &deployment,
                                                const std::string &templatePath,
                                                const std::string &storeRoot) {
    if (storeRoot.empty()) {
        return RuntimeError::make("store root is empty");
    }

    string configPath = PathDefine::getBizConfigPath(storeRoot, deployment.deploymentname());
    string tmpPath = configPath + ".tmp";

    if (fslib::fs::FileSystem::isExist(configPath) == fslib::EC_TRUE) {
        return configPath;
    }

    if (!isOk(ConfigFileUtil::rmPath(tmpPath))) {
        return RuntimeError::make("rm tmp path [%s] failed", tmpPath.c_str());
    }

    if (!genAnalyzerJson(templatePath, tmpPath)) {
        return RuntimeError::make("gen analyzer json failed");
    }

    std::vector<std::string> searcherZones;
    for (const auto &cluster : deployment.clusters()) {
        if (cluster.config().type() == ClusterConfig::CT_SEARCHER) {
            searcherZones.emplace_back(cluster.clustername());
        }
    }
    if (!genSqlJson(templatePath, tmpPath, searcherZones)) {
        return RuntimeError::make("gen analyzer json failed");
    }

    if (!genNaviScript(templatePath, tmpPath)) {
        return RuntimeError::make("gen navi script failed");
    }

    if (!isOk(ConfigFileUtil::rmPath(configPath))) {
        return RuntimeError::make("remove config path [%s]", configPath.c_str());
    }

    if (!isOk(ConfigFileUtil::mvPath(tmpPath, configPath))) {
        return RuntimeError::make("move path [%s] to [%s] failed", tmpPath.c_str(), configPath.c_str());
    }

    return configPath;
}

bool BizConfigMaker::genAnalyzerJson(const std::string &templatePath, const std::string &configRoot) {
    std::string content;
    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "analyzer.json"), &content))) {
        AUTIL_LOG(ERROR, "read analyzer file failed");
        return false;
    }
    if (!isOk(ConfigFileUtil::uploadFile(PathDefine::join(configRoot, "analyzer.json"), content))) {
        AUTIL_LOG(ERROR, "upload analyzer file failed");
        return false;
    }
    return true;
}

bool BizConfigMaker::genSqlJson(const std::string &templatePath,
                                const std::string &configRoot,
                                const std::vector<std::string> &searcherZones) {
    std::string content;
    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "sql.json"), &content))) {
        AUTIL_LOG(ERROR, "read sql file failed");
        return false;
    }
    autil::StringUtil::replaceAll(content, "$zones", autil::legacy::FastToJsonString(searcherZones));
    if (!isOk(ConfigFileUtil::uploadFile(PathDefine::join(configRoot, "sql.json"), content))) {
        AUTIL_LOG(ERROR, "upload sql file failed");
        return false;
    }
    return true;
}

bool BizConfigMaker::genNaviScript(const std::string &templatePath, const std::string &configRoot) {
    std::string content;
    if (!isOk(ConfigFileUtil::readFile(PathDefine::join(templatePath, "navi.py"), &content))) {
        AUTIL_LOG(ERROR, "read navi file failed");
        return false;
    }
    if (!isOk(ConfigFileUtil::uploadFile(PathDefine::join(configRoot, "navi.py"), content))) {
        AUTIL_LOG(ERROR, "upload navi file failed");
        return false;
    }
    return true;
}

} // namespace suez
