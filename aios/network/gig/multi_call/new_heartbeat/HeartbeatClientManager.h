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

#include "aios/network/gig/multi_call/new_heartbeat/HostHeartbeatInfo.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "autil/LoopThread.h"
#include "aios/network/anet/atomic.h"

namespace multi_call {

class SearchService;
class SearchServiceSnapshot;

typedef std::unordered_map<std::string, HostHeartbeatInfoPtr> HostHeartbeatInfoMap;
MULTI_CALL_TYPEDEF_PTR(HostHeartbeatInfoMap);

class HeartbeatClientManager;

class HeartbeatClientManagerNotifier {
public:
    HeartbeatClientManagerNotifier(HeartbeatClientManager *manager)
        : _manager(manager)
    {
        atomic_set(&_version, 0);
    }
public:
    int64_t getVersion() const {
        return atomic_read(&_version);
    }
    void notify() {
        atomic_inc(&_version);
    }
    void notifyHeartbeatSuccess() const;
    void stealManager();
private:
    atomic64_t _version;
    mutable autil::ThreadMutex _managerLock;
    HeartbeatClientManager *_manager;
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatClientManagerNotifier);

class SearchServiceManager;

class HeartbeatClientManager
{
public:
    HeartbeatClientManager(SearchService *searchService, SearchServiceManager *manager);
    ~HeartbeatClientManager();
private:
    HeartbeatClientManager(const HeartbeatClientManager &);
    HeartbeatClientManager &operator=(const HeartbeatClientManager &);
public:
    bool start(const MiscConfigPtr &miscConfig,
               const std::shared_ptr<SearchServiceSnapshot> &heartbeatSnapshot);
    void stop();
    bool updateSpecVec(const HeartbeatSpecVec &heartbeatSpecs);
    void notifyHeartbeatSuccess();
    bool fillBizInfoMap(BizInfoMap &bizInfoMap);
    int64_t getVersion() const {
        return _notifier->getVersion();
    }
    void log();
    void getSpecSet(std::set<std::string> &specSet) const;
private:
    bool initThread();
    void loop();
    void setHostMap(const HostHeartbeatInfoMapPtr &newMap);
    HostHeartbeatInfoMapPtr getHostMap() const;
    std::string toString() const;
private:
    SearchService *_searchService;
    SearchServiceManager *_manager;
    std::shared_ptr<SearchServiceSnapshot> _heartbeatSnapshot;
    mutable autil::ThreadMutex _hostMapLock;
    HostHeartbeatInfoMapPtr _hostMap;
    HeartbeatClientManagerNotifierPtr _notifier;
    autil::LoopThreadPtr _thread;
    MiscConfigPtr _miscConfig;
    std::string _snapshotStr;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatClientManager);

}
