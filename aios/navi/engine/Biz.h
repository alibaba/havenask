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

#include "navi/config/NaviConfig.h"
#include "navi/engine/CreatorManager.h"
#include "navi/engine/Data.h"
#include "navi/engine/GraphInfo.h"
#include "navi/engine/ResourceManager.h"
#include "navi/builder/GraphBuilder.h"

namespace kmonitor {
class MetricsReporter;
}

namespace navi {

class Creator;
class Kernel;
class NodeDef;
class BizManager;
class GraphBuilder;
struct NaviSnapshotSummary;
struct BizSummary;

class Biz
{
public:
    Biz(BizManager *bizManager, const std::string &bizName);
    ~Biz();
private:
    Biz(const Biz &);
    Biz &operator=(const Biz &);
public:
    bool preInit(const std::string &configPath, const NaviBizConfig &config,
                 const ModuleManagerPtr &moduleManager, const Biz *buildinBiz,
                 const Biz *oldBiz);
    bool saveResource(NaviPartId partCount, NaviPartId partId, const ResourceMap &resourceMap);
    bool collectPublish(std::vector<multi_call::ServerBizTopoInfo> &infoVec) const;
    void startPublish(multi_call::GigRpcServer *gigRpcServer);
    bool publishResource(NaviPartId partCount, NaviPartId partId);
    bool postInit(const Biz &buildinBiz);
    void initMetrics(kmonitor::MetricsReporter &reporter);
    const NaviRegistryConfigMap &getBizStageResourceConfigMap() const;
    bool initResourceGraph(bool isBuildin, GraphId buildinGraphId,
                           GraphBuilder &builder,
                           GraphId &resultGraphId) const;
    N addResourceGraph(const std::string &resource,
                       const MultiLayerResourceMap &multiLayerResourceMap,
                       int32_t scope,
                       const std::string &requireNode,
                       const std::unordered_map<std::string, std::string> &replaceMap,
                       GraphBuilder &builder) const;
    bool collectResourceMap(NaviPartId partCount, NaviPartId partId,
                            MultiLayerResourceMap &multiLayerResourceMap) const;
    ResourceStage getResourceStage(const std::string &name) const;
    ErrorCode createResource(NaviPartId partCount,
                             NaviPartId partId,
                             const std::string &name,
                             ResourceStage minStage,
                             const MultiLayerResourceMap &multiLayerResourceMap,
                             const SubNamedDataMap *namedDataMap,
                             const ProduceInfo &produceInfo,
                             NaviConfigContext *ctx,
                             bool checkReuse,
                             Node *requireKernelNode,
                             ResourcePtr &resource);
    ResourcePtr buildKernelResourceRecur(NaviPartId partCount,
                                         NaviPartId partId,
                                         const std::string &name,
                                         KernelConfigContext *ctx,
                                         const SubNamedDataMap *namedDataMap,
                                         const ResourceMap &nodeResourceMap,
                                         Node *requireKernelNode,
                                         ResourceMap &inputResourceMap);
    const CreatorManagerPtr &getCreatorManager() const;
    const KernelCreator *getKernelCreator(const std::string &name) const;
    const ResourceCreator *getResourceCreator(const std::string &name) const;
    const GraphInfo *getGraphInfo(const std::string &name) const;
    bool isSinglePart() const;
    bool hasPartId(NaviPartId partId) const;
    NaviPartId getPartCount() const;
    void getPartInfo(NaviPartId &partCount, std::vector<NaviPartId> &partIds) const;
    std::vector<NaviPartId> getPartIdsWithoutBizPart() const;
    const std::set<std::string> &getKernelSet() const;
    const std::set<std::string> &getKernelBlacklist() const;
    autil::legacy::RapidValue *getKernelConfig(const std::string &name) const;
    const std::map<std::string, bool> *getResourceDependResourceMap(const std::string &name) const;
    const std::unordered_set<std::string> *getResourceReplacerSet(const std::string &name) const;
    const std::string &getConfigPath() const;
    const std::string &getName() const;
    const BizPublishInfos &getPublishInfos() const {
        return _publishInfos;
    }
    void fillSummary(NaviSnapshotSummary &summary) const;

public:
    TestMode getTestMode() const;

private:
    bool initCreatorManager(const NaviBizConfig &config,
                            const ModuleManagerPtr &moduleManager,
                            const Biz *buildinBiz);
    void initConfigPath(const std::string &configPath,
                        const NaviBizConfig &config);
    bool initConfig(const NaviBizConfig &config, const Biz *buildinBiz);
    bool initKernelSet(const NaviBizConfig &config);
    void addBuildinKernels();
    bool initKernelCreatorMap();
    bool initResourceStageMap(const NaviBizConfig &config, const Biz *buildinBiz);
    void mergeBuildinResource(const Biz *buildinBiz);
    void addToStageMap(ResourceStage stage, const std::string &resource,
                       bool require);
    bool createResourceManager(const NaviBizConfig &config,
                               const Biz *buildinBiz, const Biz *oldBiz);
    bool loadGraphs(const NaviBizConfig &config);
    bool initPublishInfos(const NaviBizConfig &config);
    ResourceManager *getResourceManager(NaviPartId partId) const;
    void fillKernelSummary(BizSummary &bizSummary) const;
    void fillResourceSummary(BizSummary &bizSummary) const;
private:
    BizManager *_bizManager;
    std::string _bizName;
    std::string _configPath;
    CreatorManagerPtr _creatorManager;
    std::set<std::string> _kernelSet;
    std::set<std::string> _kernelBlacklist;
    BizPublishInfos _publishInfos;
    ResourceStageMap _resourceStageMap;
    std::unordered_map<std::string, const KernelCreator *> _kernelCreatorMap;
    NaviPartId _partCount;
    std::unordered_map<NaviPartId, ResourceManager *> _resourceManagers;
    NaviRegistryConfigMap _kernelConfigMap;
    NaviRegistryConfigMap _resourceConfigMap;
    NaviRegistryConfigMap _bizStageResourceConfigMap;
    std::unordered_map<std::string, GraphInfo *> _graphInfoMap;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
};

NAVI_TYPEDEF_PTR(Biz);
typedef std::unordered_map<std::string, BizPtr> BizMap;
NAVI_TYPEDEF_PTR(BizMap);

}

#endif //NAVI_BIZ_H
