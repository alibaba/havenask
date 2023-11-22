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

#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/new_heartbeat/BizTopo.h"

namespace multi_call {

class SearchService;
class SearchServiceSnapshot;
class NewHeartbeatResponse;
class HeartbeatClientManagerNotifier;
class HeartbeatClientStream;

class HostHeartbeatStats
{
public:
    void updateLastHeartbeatSendTime();
    void updateLastResponseTime();
    void updateLastHeartbeatTime(bool isSuccess);
    bool isHeartbeatTimeout();
    bool checkSubscribeTimeout(int64_t currentTime);
    bool skip(int64_t currentTime);
    void incTotalCount();
private:
    bool skipByCount();
public:
    std::string addr;
    std::shared_ptr<HeartbeatClientManagerNotifier> notifier;
    bool success = false;
    bool topoReady = false;
    bool subscribeTimeout = false;
    bool lastCheckSuccess = false;
    bool ignoreNextSkip = false;
    int64_t firstHeartbeatTime = 0;
    int64_t lastHeartbeatSendTime = 0;
    int64_t lastHeartbeatTime = 0;
    int64_t lastResponseTime = 0;
    int64_t skipCount = 0;
    int64_t totalCount = 0;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HostHeartbeatStats);

class HostHeartbeatInfo
{
public:
    HostHeartbeatInfo();
    ~HostHeartbeatInfo();

private:
    HostHeartbeatInfo(const HostHeartbeatInfo &);
    HostHeartbeatInfo &operator=(const HostHeartbeatInfo &);

public:
    bool init(const std::shared_ptr<HeartbeatClientManagerNotifier> &notifier,
              const std::shared_ptr<SearchServiceSnapshot> &heartbeatSnapshot,
              SearchService *searchService, const HeartbeatSpec &spec);
    bool tick();
    void stop();
    bool fillBizInfoMap(BizInfoMap &bizInfoMap) const;
    void toString(std::string &debugStr, MetasSignatureMap &allMetas) const;
    bool isTopoReady() const;

private:
    void checkHeartbeatTimeout();
    bool createStream();
    std::shared_ptr<HeartbeatClientStream> newClientStream() const;
    std::shared_ptr<HeartbeatClientStream> getStream() const;
    void setStream(const std::shared_ptr<HeartbeatClientStream> &stream);

private:
    std::shared_ptr<SearchServiceSnapshot> _heartbeatSnapshot;
    SearchService *_searchService;
    HeartbeatSpec _spec;
    mutable autil::ReadWriteLock _streamLock;
    std::shared_ptr<HeartbeatClientStream> _stream;
    HostHeartbeatStatsPtr _stats;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HostHeartbeatInfo);

} // namespace multi_call
