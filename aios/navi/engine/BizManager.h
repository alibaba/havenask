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
struct NaviSnapshotSummary;
struct BizSummary;

class BizManager
{
public:
    BizManager(const std::string &naviName,
               const std::shared_ptr<ModuleManager> &moduleManager);
    ~BizManager();
private:
    BizManager(const BizManager &);
    BizManager &operator=(const BizManager &);
public:
    bool preInit(const NaviLoggerPtr &logger, BizManager *oldBizManager,
                 const NaviConfig &config, const ResourceMap &rootResourceMapIn);
    GraphDef *cloneResourceGraph() const;
    bool postInit();
    bool start(multi_call::GigRpcServer *gigRpcServer, BizManager *oldBizManager);
    void initMetrics(kmonitor::MetricsReporter &reporter);
    GraphDef *createResourceGraph(const std::string &bizName,
                                  NaviPartId partCount,
                                  NaviPartId partId,
                                  const std::set<std::string> &resourceSet) const;
    bool isLocal(const LocationDef &location) const;
    BizPtr getBiz(NaviLogger *_logger, const std::string &bizName) const;
    bool getBizPartInfo(const std::string &bizName, NaviPartId &partCount, std::vector<NaviPartId> &partIds) const;
    const BizPtr &getBuildinBiz() const;
    const ModuleManagerPtr &getModuleManager() const;
    const CreatorManagerPtr &getBuildinCreatorManager() const;
    ResourcePtr getSnapshotResource(const std::string &name) const;
    void fillSummary(NaviSnapshotSummary &summary) const;
    void cleanupModule();
    void stealPublishSignature(const std::vector<multi_call::SignatureTy> &stealList);

public:
    void setTestMode(TestMode testMode) { _testMode = testMode; }
    TestMode getTestMode() const { return _testMode; }

private:
    bool initModuleManager(BizManager *oldBizManager);
    bool createBuildinBiz(const NaviConfig &config);
    void initBuildinBizResourceList(
        const std::vector<NaviRegistryConfig> &configVec);
    bool initMemoryPoolR(const ResourceMap &rootResourceMap);
    bool initTypeMemoryPool();
    bool createBizs(const NaviConfig &config,
                    BizManager *oldBizManager);
    BizPtr createBiz(const std::string &bizName,
                     const std::string &configPath,
                     const NaviBizConfig &config,
                     const Biz *buildinBiz,
                     const Biz *oldBiz);
    bool initResourceGraph(const NaviConfig &config);
    BizPtr doGetBiz(const std::string &bizName) const;
    const BizMapPtr &getBizMap() const;
    void logUpdateGraphDef() const;
private:
    DECLARE_LOGGER();
    TestMode _testMode = TM_NOT_TEST;
    std::string _naviName;
    std::vector<multi_call::SignatureTy> _publishSignatures;
    multi_call::GigRpcServer *_gigRpcServer = nullptr;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    BizPtr _buildinBiz;
    NaviBizConfig _buildinConfig;
    ResourceMap _memoryPoolRMap;
    MultiLayerResourceMap _snapshotMultiLayerResourceMap;
    BizMap _bizMap;
    BizPtr _defaultBiz;
    ModuleManagerPtr _moduleManager;
    GraphDef *_graphDef;
};

NAVI_TYPEDEF_PTR(BizManager);

}

#endif //NAVI_BIZMANAGER_H
