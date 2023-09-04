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
#ifndef ISEARCH_MULTI_CALL_HBINFO_H
#define ISEARCH_MULTI_CALL_HBINFO_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/Heartbeat.pb.h"
#include "autil/Lock.h"

namespace multi_call {

struct HbMetaInfo {
    Meta meta;
    // VersionTy version;
};

MULTI_CALL_TYPEDEF_PTR(HbMetaInfo);

struct HbInfo {
public:
    HbInfo() : heartbeatProtocol(MC_PROTOCOL_UNKNOWN), heartbeatSpec(""), lastUpdateTime(0) {
    }

public:
    ProtocolType heartbeatProtocol;
    std::string heartbeatSpec;

public:
    void updateHbMeta(const HbMetaInfoPtr &hbMetaPtr);
    HbMetaInfoPtr getHbMeta(); // return copy of HbMetaInfoPtr
    void updateTime();
    void updateTime(int64_t time);
    int64_t getTime() {
        return lastUpdateTime;
    }

private:
    int64_t lastUpdateTime;
    autil::SpinLock _lock;
    HbMetaInfoPtr hbMeta;
};

MULTI_CALL_TYPEDEF_PTR(HbInfo);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_HBINFO_H
