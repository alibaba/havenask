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
#ifndef NAVI_BIZ_H
#define NAVI_BIZ_H

#include "navi/builder/GraphBuilder.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/CreatorManager.h"
#include "navi/engine/Data.h"
#include "navi/engine/GraphInfo.h"
#include "navi/engine/ResourceManager.h"

namespace kmonitor {
class MetricsReporter;
}

namespace navi {

class Creator;
class Kernel;
class NodeDef;
class BizManager;

class Biz
{
public:
    Biz(BizManager *bizManager, const std::string &bizName,
        const CreatorManager &creatorManager);
    ~Biz();
private:
    Biz(const Biz &);
    Biz &operator=(const Biz &);
public:
    bool preInit(const std::string &configPath, const NaviBizConfig &config,
                 const Biz *oldBiz);
    bool saveInitResource(NaviPartId partCount, NaviPartId partId,
                          const ResourceMap &resourceMap);
    bool postInit(const Biz &defaultBiz);
    void addResourceGraph(const std::string &resource,
                          const MultiLayerResourceMap &multiLayerResourceMap,
                          GraphBuilder &builder) const;
    void collectResourceMap(NaviPartId partCount, NaviPartId partId,
                            MultiLayerResourceMap &multiLayerResourceMap) const;
    ResourcePtr createResource(NaviPartId partCount,
                               NaviPartId partId,
                               const std::string &name,
                               const MultiLayerResourceMap &multiLayerResourceMap);
    const KernelCreator *getKernelCreator(const std::string &name) const;
    const ResourceCreator *getResourceCreator(const std::string &name) const;
    const GraphInfo *getGraphInfo(const std::string &name) const;
    bool isSinglePart() const;
    bool hasPartId(NaviPartId partId) const;
    autil::legacy::RapidValue *getKernelConfig(const std::string &name) const;
    const std::map<std::string, bool> *getKernelDependResourceMap(
            const std::string &name) const;
    const std::map<std::string, bool> *getResourceDependResourceMap(
            const std::string &name) const;
    const std::string &getConfigPath() const;
    const std::string &getName() const;
private:
    void initConfigPath(const std::string &configPath,
                        const NaviBizConfig &config);
    bool initConfig(const NaviBizConfig &config);
    bool createResourceManager(const NaviBizConfig &config, const Biz *oldBiz);
    bool loadGraphs(const NaviBizConfig &config);
    ResourceManager *getResourceManager(NaviPartId partId) const;
private:
    BizManager *_bizManager;
    std::string _bizName;
    std::string _configPath;
    const CreatorManager &_creatorManager;
    NaviPartId _partCount;
    std::unordered_map<NaviPartId, ResourceManager *> _resourceManagers;
    NaviRegistryConfigMap _kernelConfigMap;
    NaviRegistryConfigMap _resourceConfigMap;
    std::unordered_map<std::string, GraphInfo *> _graphInfoMap;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
};

NAVI_TYPEDEF_PTR(Biz);
typedef std::unordered_map<std::string, BizPtr> BizMap;
NAVI_TYPEDEF_PTR(BizMap);

}

#endif //NAVI_BIZ_H
