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
#include "suez/admin/TargetGenerator.h"

#include "autil/EnvUtil.h"
#include "autil/result/Errors.h"
#include "suez/sdk/PathDefine.h"

using namespace catalog;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TargetGenerator);

TargetGenerator::TargetGenerator(const std::shared_ptr<catalog::CatalogControllerManager> &catalogControllerManager,
                                 const std::shared_ptr<ClusterManager> &clusterManager)
    : _catalogControllerManager(catalogControllerManager)
    , _clusterManager(clusterManager)
    , _templatePath(autil::EnvUtil::getEnv("DIRECT_WRITE_TEMPLATE_CONFIG_PATH",
                                           std::string("/usr/local/etc/catalog/direct_write_templates"))) {}

TargetGenerator::~TargetGenerator() = default;

autil::Result<AdminTarget> TargetGenerator::genTarget() {
    AdminTarget target;
    auto clusterDeploymentMap = _clusterManager->getClusterDeploymentMap();
    auto dbDeploymentMap = _clusterManager->getDbDeploymentMap();

    // clustername 在不同deployment下假设也是唯一的
    bool sqlMode = false;
    for (const auto &iter : clusterDeploymentMap) {
        auto configRoot = iter.second.config().configroot();
        for (const auto &cluster : iter.second.clusters()) {
            if (cluster.config().type() == ClusterConfig::CT_QRS) {
                sqlMode = true;
            }
        }
    }

    for (const auto &iter : clusterDeploymentMap) {
        auto configRoot = iter.second.config().configroot();
        for (const auto &cluster : iter.second.clusters()) {
            ZoneTarget zoneTarget;
            if (!zoneTarget.fillZoneInfo(cluster)) {
                return autil::result::RuntimeError::make("fill zone info failed, zone [%s]",
                                                         cluster.clustername().c_str());
            }
            if (sqlMode && !zoneTarget.fillBizInfo(iter.second,
                                                   PathDefine::join(_templatePath, "biz_config"),
                                                   configRoot,
                                                   _lastBizConfigSignature,
                                                   _lastBizConfigPath)) {
                return autil::result::RuntimeError::make("fill biz info failed, zone [%s], config root [%s]",
                                                         cluster.clustername().c_str(),
                                                         configRoot.c_str());
            }
            target.zones.emplace(cluster.clustername(), zoneTarget);
        }
    }

    for (const auto &iter : dbDeploymentMap) {
        const auto &deployment = iter.second;
        for (const auto &binding : deployment.bindings()) {
            auto catalogController = _catalogControllerManager->get(deployment.catalogname());
            if (catalogController == nullptr) {
                return autil::result::RuntimeError::make("not found catalog [%s]", deployment.catalogname().c_str());
            }
            Catalog catalog;
            if (!isOk(catalogController->getCatalog(&catalog))) {
                return autil::result::RuntimeError::make("not found catalog snapshot [%s]",
                                                         deployment.catalogname().c_str());
            }
            Database *db = nullptr;
            if (!isOk(catalog.getDatabase(deployment.databasename(), db))) {
                return autil::result::RuntimeError::make("not found database [%s]", deployment.databasename().c_str());
            }
            if (target.zones.count(binding.clustername()) == 0) {
                return autil::result::RuntimeError::make("not found cluster [%s]", binding.clustername().c_str());
            }
            auto &zoneTarget = target.zones[binding.clustername()];
            std::vector<const Table *> tables;
            if (!isOk(db->getTableGroupTables(binding.tablegroupname(), tables))) {
                return autil::result::RuntimeError::make("get table in tablegroup [%s] failed",
                                                         binding.tablegroupname().c_str());
            }
            const auto &builds = catalogController->getCurrentBuilds();
            if (!zoneTarget.fillTableInfos(db->databaseConfig().store_root(), _templatePath, tables, builds)) {
                return autil::result::RuntimeError::make("fill table infos failed");
            }

            if (sqlMode) {
                const auto &deploymentIter = clusterDeploymentMap.find(deployment.deploymentname());
                if (deploymentIter == clusterDeploymentMap.end()) {
                    return autil::result::RuntimeError::make("not found cluster %s", binding.clustername().c_str());
                }
            }
        }
    }

    return target;
}

} // namespace suez
