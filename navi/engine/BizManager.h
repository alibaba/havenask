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
#ifndef NAVI_BIZMANAGER_H
#define NAVI_BIZMANAGER_H

#include "navi/common.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/Biz.h"
#include "navi/engine/CreatorManager.h"
#include "autil/Lock.h"
#include <map>

namespace navi {

class ResourceMap;
class GraphMemoryPoolResource;
class MetaInfoResourceBase;

class BizManager
{
public:
    BizManager(const std::shared_ptr<CreatorManager> &creatorManager);
    ~BizManager();
private:
    BizManager(const BizManager &);
    BizManager &operator=(const BizManager &);
public:
    bool preInit(const NaviLoggerPtr &logger, BizManager *oldBizManager,
                 const NaviConfig &config, const ResourceMap &rootResourceMapIn);
    GraphDef *stealResourceGraph();
    bool postInit();
    void initMetrics(kmonitor::MetricsReporter &reporter);
    void addResourceGraph(const Biz *biz,
                          const std::string &resource,
                          const MultiLayerResourceMap &multiLayerResourceMap,
                          GraphBuilder &builder) const;
    GraphDef *createResourceGraph(const std::string &bizName,
                                  NaviPartId partCount,
                                  NaviPartId partId,
                                  const std::set<std::string> &resourceSet) const;
    bool isLocal(const LocationDef &location) const;
    BizPtr getBiz(NaviLogger *_logger, const std::string &bizName) const;
    bool isDefaultBiz(const BizPtr &biz) const;
    const CreatorManagerPtr &getCreatorManager() const;
    ResourcePtr getRootResource(const std::string &name) const;
    std::shared_ptr<GraphMemoryPoolResource> createGraphMemoryPoolResource() const;
    std::shared_ptr<MetaInfoResourceBase> createMetaInfoResource() const;
private:
    bool initCreatorManager(const NaviConfig &config);
    const ResourceCreator *getResourceCreator(const std::string &name) const;
    bool createDefaultBiz(const NaviConfig &config);
    void initDefaultBizResourceList(const std::vector<NaviRegistryConfig> &configVec,
                                    const std::set<std::string> &initResources);
    bool saveMemoryPoolResource(const ResourceMap &rootResourceMap);
    bool createBizs(const NaviConfig &config,
                    BizManager *oldBizManager);
    BizPtr createBiz(const std::string &bizName,
                     const std::string &configPath,
                     const NaviBizConfig &config,
                     const Biz *oldBiz);
    bool initResourceGraph(const NaviConfig &config,
                           const ResourceMap &rootResourceMap);
    GraphId initDefaultGraph(const MultiLayerResourceMap &multiLayerResourceMap,
                             GraphBuilder &builder);
    void addDependResourceRecur(const Biz *biz,
                                const std::string &resource,
                                const MultiLayerResourceMap &multiLayerResourceMap,
                                GraphBuilder &builder) const;
    void addResourceGraphWithDefault(
        const Biz *biz, const std::string &resource, GraphId defaultGraphId,
        const MultiLayerResourceMap &multiLayerResourceMap,
        GraphBuilder &builder) const;
    void addDependResourceWithDefaultRecur(
        const Biz *biz, const std::string &resource, GraphId defaultGraphId,
        const MultiLayerResourceMap &multiLayerResourceMap,
        GraphBuilder &builder) const;
    BizPtr doGetBiz(const std::string &bizName) const;
    const BizMapPtr &getBizMap() const;
private:
    DECLARE_LOGGER();
    BizPtr _defaultBiz;
    NaviBizConfig _defaultConfig;
    ResourceMap _memoryPoolResourceMap;
    MultiLayerResourceMap _rootMultiLayerResourceMap;
    BizMap _bizMap;
    CreatorManagerPtr _creatorManager;
    GraphDef *_graphDef;
};

NAVI_TYPEDEF_PTR(BizManager);

}

#endif //NAVI_BIZMANAGER_H
