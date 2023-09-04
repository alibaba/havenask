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
#ifndef MULTI_CALL_SEARCHSERVICEMANAGER_H
#define MULTI_CALL_SEARCHSERVICEMANAGER_H
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/config/SubscribeClustersConfig.h"
#include "aios/network/gig/multi_call/interface/Closure.h"
#include "aios/network/gig/multi_call/metric/WorkerMetricReporter.h"
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientManager.h"
#include "aios/network/gig/multi_call/service/ConnectionManager.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInBiz.h"
#include "aios/network/gig/multi_call/subscribe/SubscribeServiceManager.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"

namespace multi_call {

class SearchService;
class LatencyTimeSnapshot;
MULTI_CALL_TYPEDEF_PTR(LatencyTimeSnapshot);

class SearchServiceManager
{
public:
    SearchServiceManager(SearchService *searchService = nullptr);
    // virtual for ut
    virtual ~SearchServiceManager();

private:
    SearchServiceManager(const SearchServiceManager &);
    SearchServiceManager &operator=(const SearchServiceManager &);

public:
    // virtual for ut
    virtual bool init(const MultiCallConfig &mcConfig);
    SearchServiceSnapshotPtr getSearchServiceSnapshot() const;
    void setMetricReporterManager(const MetricReporterManagerPtr &metricReporterManager) {
        _metricReporterManager = metricReporterManager;
        _subscribeServiceManager->setMetricReporterManager(metricReporterManager);
    }
    void setLatencyTimeSnapshot(const LatencyTimeSnapshotPtr &snapshot) {
        _latencyTimeSnapshot = snapshot;
    }
    void stop();
    void startLogThread();
    void stopLogThread();
    SubscribeServiceManagerPtr getSubscribeServiceManager() const {
        return _subscribeServiceManager;
    }
    void notifyUpdate() const;
    bool setSnapshotChangeCallback(SnapshotChangeCallback *callback);
    void stealSnapshotChangeCallback();

public:
    // for java client
    bool createSnapshot(const TopoNodeVec &topoNodeVec);
    void updateStaticParam();
    const MiscConfigPtr &getMiscConfig() const {
        return _miscConfig;
    }
    bool addSubscribeService(const SubscribeConfig &subServiceConf);
    bool addSubscribe(const SubscribeClustersConfig &gigSubConf);
    bool deleteSubscribe(const SubscribeClustersConfig &gigSubConf);
    const ConnectionManagerPtr &getConnectionManager() const {
        return _connectionManager;
    }

public:
    void reportMetrics();
    // only for wunder, may can replace
    virtual void enableSubscribeService(SubscribeType ssType);
    virtual void disableSubscribeService(SubscribeType ssType);
    virtual void stopSubscribeService(SubscribeType ssType);
    virtual void waitCreateSnapshotLoop(int64_t waitTime = UPDATE_TIME_INTERVAL);

private:
    // virtual for ut
    virtual void update();
    virtual bool initConnectionManager(const ConnectionConfig &conf, const MiscConfig &miscConf);
    bool initHeartbeat();
    virtual bool waitCreateSnapshot(int64_t waitTime);
    void setSearchServiceSnapshot(const SearchServiceSnapshotPtr &searchServiceSnapshotPtr);
    bool needReCreateSnapshot();
    void createSnapshot();
    SearchServiceSnapshotPtr constructSnapshot(const BizInfoMap &bizInfoMap,
                                               const SearchServiceSnapshotPtr &oldSnapshot);
    bool getSpecSet(std::set<std::string> &specSet) const;
    void syncConnection() const;
    void logThread();
    void logSnapshot(const SearchServiceSnapshotPtr &newSnapshot);
    void stopUpdateThread();
    void setUpdateThread(const autil::LoopThreadPtr &updateThread);
    autil::LoopThreadPtr getUpdateThread() const;
    void runSnapshotChangeCallback();

private:
    static void constructBizInfoMap(const TopoNodeVec &topoNodeVec, BizInfoMap &bizInfoMap);
    static ConnectionConfig addDefaultGrpcConfig(const ConnectionConfig &conf);

private:
    mutable autil::ReadWriteLock _commonLock;
    mutable autil::ReadWriteLock _snapshotLock;
    autil::ThreadCond _snapshotWaitCond;
    SearchServiceSnapshotPtr _searchServiceSnapshotPtr;
    SubscribeServiceManagerPtr _subscribeServiceManager;
    LatencyTimeSnapshotPtr _latencyTimeSnapshot;
    mutable autil::ThreadMutex _updateThreadLock;
    autil::LoopThreadPtr _updateThreadPtr;
    autil::LoopThreadPtr _snapshotLogThreadPtr;
    MetricReporterManagerPtr _metricReporterManager;
    ConnectionManagerPtr _connectionManager;
    std::string _snapshotStr;
    std::string _newSnapshotStr;
    MiscConfigPtr _miscConfig;
    HeartbeatClientManagerPtr _heartbeatClientManager;
    bool _onlyHeartbeatSub;
    VersionTy _heartbeatVersion;
    mutable autil::ThreadMutex _callbackLock;
    SnapshotChangeCallback *_callback;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SearchServiceManager);

} // namespace multi_call

#endif // MULTI_CALL_SEARCHSERVICEMANAGER_H
