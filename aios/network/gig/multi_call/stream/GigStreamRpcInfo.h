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

#include <map>

#include "aios/network/anet/atomic.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "autil/FmtStringUtil.h"
#include "autil/Lock.h"

namespace autil {
class DataBuffer;
}

namespace multi_call {
class GigStreamHandlerBase;

enum GigStreamRpcStatus {
    GSRS_NONE,
    GSRS_EOF,
    GSRS_CANCELLED,
};

const char *convertGigStreamRpcStatus(GigStreamRpcStatus status);

struct GigStreamRpcInfo {
public:
    GigStreamRpcInfo(int64_t partId = 0);

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

public:
    bool isLack() const;

public:
    friend std::ostream &operator<<(std::ostream &os,
                                    const GigStreamRpcInfo &rpcInfo);

public:
    bool operator==(const GigStreamRpcInfo &other) const;

public:
    int64_t partId;
    int64_t sendCount = 0;
    int64_t receiveCount = 0;
    int64_t sendBeginTs = 0;
    int64_t sendEndTs = 0;
    GigStreamRpcStatus sendStatus = GSRS_NONE;
    int64_t receiveBeginTs = 0;
    int64_t receiveEndTs = 0;
    GigStreamRpcStatus receiveStatus = GSRS_NONE;
    std::string spec;
    bool isRetry = false;
};

class GigStreamRpcRecord {
public:
    void begin();
    void eof();
    void cancel();
    void snapshot(GigStreamRpcStatus &status, int64_t &beginTs,
                  int64_t &endTs) const;
private:
    int64_t _beginTs = 0;
    int64_t _endTs = 0;
    GigStreamRpcStatus _status = GSRS_NONE;
    mutable autil::ReadWriteLock _lock;
};

class GigStreamRpcInfoController {
public:
    GigStreamRpcInfoController(PartIdTy partId);
    ~GigStreamRpcInfoController();

private:
    GigStreamRpcInfoController(const GigStreamRpcInfoController &);
    GigStreamRpcInfoController &operator=(const GigStreamRpcInfoController &);

public:
    friend class GigStreamHandlerBase;
    friend class GrpcClientStreamHandler;

public:
    void snapshot(GigStreamRpcInfo &rpcInfo) const;

private:
    PartIdTy _partId;
    atomic64_t _closureCount;
    atomic64_t _totalSendClosureCount;
    atomic64_t _totalReceiveClosureCount;
    atomic64_t _totalInitClosureCount;
    atomic64_t _totalFinishClosureCount;
    atomic64_t _totalCancelClosureCount;
    atomic64_t _totalSendCount;
    atomic64_t _totalReceiveCount;
    atomic64_t _totalSendSize;
    atomic64_t _totalReceiveSize;
    GigStreamRpcRecord _sendRecord;
    GigStreamRpcRecord _receiveRecord;
};

typedef std::vector<GigStreamRpcInfo> GigStreamRpcInfoVec;
typedef std::pair<std::string, std::string> GigStreamRpcInfoKey;
typedef std::map<GigStreamRpcInfoKey, GigStreamRpcInfoVec> GigStreamRpcInfoMap;

void calcStreamRpcConverage(const GigStreamRpcInfoMap &rpcInfo,
                            size_t &usedRpcCount, size_t &lackRpcCount);

void calcStreamRpcConverage(const GigStreamRpcInfoVec &rpcInfoVec,
                            size_t &usedRpcCount, size_t &lackRpcCount);

} // namespace multi_call

template <>
struct fmt::formatter<multi_call::GigStreamRpcInfo>
    : autil::FormatterWithDefaultParse {
    template <typename FormatContext>
    auto format(const multi_call::GigStreamRpcInfo &rpcInfo,
                FormatContext &ctx) {
        return format_to(
            ctx.out(),
            "[part:{} durUs:{} SS:{} RS:{} SPEC:{} SC:{} "
            "RC:{} SBT:{} SET:{} RBT:{} RET:{} Retry:{}]",
            rpcInfo.partId, rpcInfo.receiveEndTs - rpcInfo.sendBeginTs,
            convertGigStreamRpcStatus(rpcInfo.sendStatus),
            convertGigStreamRpcStatus(rpcInfo.receiveStatus), rpcInfo.spec,
            rpcInfo.sendCount, rpcInfo.receiveCount, rpcInfo.sendBeginTs,
            rpcInfo.sendEndTs, rpcInfo.receiveBeginTs, rpcInfo.receiveEndTs,
            rpcInfo.isRetry);
    }
};
