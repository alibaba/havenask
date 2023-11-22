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
#ifndef NAVI_NAVI_H
#define NAVI_NAVI_H

#include <unordered_set>

#include "autil/LoopThread.h"
#include "autil/ObjectTracer.h"
#include "navi/config/NaviConfig.h"
#include "navi/engine/NaviUserResult.h"
#include "navi/engine/ResourceMap.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/log/NaviLogger.h"

namespace multi_call {
class GigRpcServer;
class GigStreamBase;
}

namespace kmonitor {
class MetricsReporter;
}

namespace navi {

class NaviGraphRunner;
class GraphDef;
class RunGraphParams;
class ModuleManager;
class NaviMessage;
class NaviClosure;
class NaviSnapshot;
class NaviServerStreamCreator;
class NaviHostInfo;

class Navi : public autil::ObjectTracer<Navi, true>
{
public:
    Navi(const std::string &name = "");
    ~Navi();
private:
    Navi(const Navi &);
    Navi &operator=(const Navi &);
public:
    void initMetrics(kmonitor::MetricsReporter &reporter);
    // init/stop
    bool init(const std::string &initConfigPath,
              multi_call::GigRpcServer *gigRpcServer);
    bool update(const std::string &configLoader,
                const std::string &configPath,
                const std::string &loadParam,
                const ResourceMap &rootResourceMap);
    bool update(const std::string &configStr,
                const ResourceMap &rootResourceMapIn);
    bool update(const NaviConfig &config,
                const ResourceMap &rootResourceMap);
    void stopSnapshot();
    void stop();
    std::string getTopoInfoStr() const;
public:
    // run
    NaviUserResultPtr runLocalGraph(GraphDef *graphDef,
                                    const RunGraphParams &params,
                                    const ResourceMap &resourceMap);
    void runLocalGraphAsync(GraphDef *graphDef,
                            const RunGraphParams &params,
                            const ResourceMap &resourceMap,
                            NaviUserResultClosure *closure);
    NaviUserResultPtr runGraph(GraphDef *graphDef,
                               const RunGraphParams &params);
private:
    bool initHostInfo(const std::string &installRoot, multi_call::GigRpcServer *gigRpcServer);
    bool initMetricsReportThread();
    bool initMemoryPoolR();
    void initInstanceId();
    bool initRpc(multi_call::GigRpcServer *gigRpcServer);
    void initDefaultLogger();
    void updateSnapshot(const std::shared_ptr<NaviSnapshot> &newSnapshot);
    void cleanupModule();
    std::shared_ptr<NaviSnapshot> getSnapshot() const;
    NaviUserResultPtr doRunGraph(GraphDef *graphDef,
                                 const RunGraphParams &params,
                                 const ResourceMap *resourceMap);
    void doRunGraphAsync(GraphDef *graphDef,
                         const RunGraphParams &params,
                         const ResourceMap *resourceMap,
                         NaviUserResultClosure *closure);
    bool parseRunParams(const RunParams &pbParams, RunGraphParams &params);
    std::string getNaviPythonPath();
    void reportMetricsLoop();
    void logSnapshotSummary() const;
    void waitUntilNoQuery();
public:
    // public for test
    bool update(const std::string &configLoader,
                const std::string &configPath,
                const std::string &loadParam,
                const ResourceMap &rootResourceMap,
                const std::shared_ptr<ModuleManager> &moduleManager);
    bool update(const std::string &configStr,
                const ResourceMap &rootResourceMapIn,
                const std::shared_ptr<ModuleManager> &moduleManager);
    std::shared_ptr<ModuleManager> getModuleManager() const;
    InstanceId getInstanceId() const;
    void setTestMode(TestMode testMode);
    void setWaitNoQuery(bool needWait);
    bool createResource(const std::string &bizName,
                        NaviPartId partCount,
                        NaviPartId partId,
                        const std::set<std::string> &resources,
                        ResourceMap &resourceMap) const;
private:
    friend class NaviServerStream;
    friend class NaviGraphRunner;
private:
    DECLARE_LOGGER();
    std::string _name;
    std::string _initConfigPath;
    std::shared_ptr<NaviHostInfo> _hostInfo;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    multi_call::GigRpcServer *_gigRpcServer;
    std::shared_ptr<NaviServerStreamCreator> _streamCreator;
    InstanceId _instanceId;
    autil::LoopThreadPtr _metricsReportThread;
    std::shared_ptr<Resource> _memoryPoolR;
    mutable autil::ReadWriteLock _snapshotLock;
    std::shared_ptr<NaviSnapshot> _snapshot;
    TestMode _testMode;
    bool _waitNoQuery = true;
};

NAVI_TYPEDEF_PTR(Navi);

}

#endif //NAVI_NAVI_H
