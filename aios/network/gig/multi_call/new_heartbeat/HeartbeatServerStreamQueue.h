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

#include <unordered_map>

#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerStream.h"
#include "autil/Thread.h"

namespace multi_call {

typedef std::unordered_map<int64_t, HeartbeatServerStreamPtr> HeartbeatServerStreamMap;
MULTI_CALL_TYPEDEF_PTR(HeartbeatServerStreamMap);

class HeartbeatServerStreamQueue
{
public:
    HeartbeatServerStreamQueue(size_t id);
    ~HeartbeatServerStreamQueue();

public:
    bool init();
    void stop();
    void notify();
    void addStream(const HeartbeatServerStreamPtr &stream);
    bool isEmpty() const;
    bool isAllReplicaStopped(SignatureTy sig) const;

private:
    void flush();
    void doFlush();
    void removeStream(const std::vector<int64_t> &streamVec);
    HeartbeatServerStreamMapPtr getStreamMap() const;
    void setStreamMap(const HeartbeatServerStreamMapPtr &newMap);

private:
    size_t _id;
    volatile bool _run;
    mutable autil::ThreadCond _threadCond;
    autil::ThreadPtr _thread;
    mutable autil::ThreadMutex _streamMutex;
    HeartbeatServerStreamMapPtr _streamMap;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(HeartbeatServerStreamQueue);

} // namespace multi_call
