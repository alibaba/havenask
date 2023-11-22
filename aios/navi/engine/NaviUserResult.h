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

#include "navi/engine/Data.h"
#include "navi/engine/NaviResult.h"
#include "aios/network/anet/atomic.h"

namespace navi {

struct NaviUserData {
public:
    NaviUserData()
        : partId(INVALID_NAVI_PART_ID)
        , index(0)
        , streamId(0)
        , eof(false)
    {
    }
public:
    std::string name;
    NaviPartId partId;
    size_t index;
    size_t streamId;
    DataPtr data;
    bool eof;
};

class ReadyBitMap;
class GraphDomainUser;
class GraphDomainClient;
class Port;
class NaviWorkerBase;
class Graph;
class NaviSession;
class NaviSnapshot;
class GraphResult;

template <typename T>
class DomainHolder;

struct PortDataInfo {
public:
    PortDataInfo(Port *port_, NaviPartId partId_)
        : port(port_)
        , partId(partId_)
        , streamId(0)
    {
    }
public:
    Port *port;
    NaviPartId partId;
    size_t streamId;
};

class NaviUserResult : public std::enable_shared_from_this<NaviUserResult>
{
public:
    NaviUserResult();
    virtual ~NaviUserResult();
private:
    NaviUserResult(const NaviUserResult &);
    NaviUserResult &operator=(const NaviUserResult &);
public:
    // return true if data is valid
    // eof always indicates the end of data
    virtual bool nextData(NaviUserData &data, bool &eof) = 0;
    // call this if eof return true
    NaviResultPtr getNaviResult();
    void abort(ErrorCode ec = EC_ABORT);
    size_t outputCount() const;
private:
    void setGraphResult(const std::shared_ptr<GraphResult> &result);
    const std::shared_ptr<GraphResult> &getGraphResult() const;
    NaviResultPtr getNaviResultWithoutCheck();
    bool bindDomain(const std::shared_ptr<GraphDomainUser> &domain);
    virtual void notify(bool finish) = 0;
    void notifyData();
    bool hasOutput() const;
    void setWorker(NaviWorkerBase *worker);
    bool checkFinish() const;
private:
    bool collectPort(const std::shared_ptr<GraphDomainUser> &domain);
    void initFinishMap(uint32_t portCount);
    void updateError(ErrorCode ec);
private:
    friend class KernelTester;
    friend class ArpcGraphClosure;
protected:
    bool nextPort(NaviUserData &userData, bool &eof, ErrorCode &ec);
    void terminate(ErrorCode ec);
protected:
    DECLARE_LOGGER();
    autil::ThreadCond _cond;
    bool _finished;
    std::shared_ptr<GraphResult> _graphResult;
    volatile atomic64_t _dataCount;
    mutable autil::ThreadMutex _resultMutex;
    NaviResultPtr _naviResult;
private:
    DomainHolder<GraphDomainUser> *_domainHolder;
    std::vector<PortDataInfo> _portVec;
    ReadyBitMap *_finishMap;
    size_t _loopIndex;
    std::atomic<bool> _terminated;
    // do not use _worker, gdb only
    NaviWorkerBase *_gdbPtr;
private:
    friend class GraphDomainUser;
    friend class GraphDomainClient;
    friend class NaviWorkerBase;
    friend class Graph;
    friend class NaviSession;
    friend class NaviSnapshot;
};

NAVI_TYPEDEF_PTR(NaviUserResult);

class NaviUserResultClosure {
public:
    NaviUserResultClosure() = default;
    virtual ~NaviUserResultClosure() = default;
public:
    virtual void run(NaviUserResultPtr result) = 0;
};

class NaviSyncUserResult : public NaviUserResult {
public:
    virtual bool nextData(NaviUserData &data, bool &eof) override;
private:
    virtual void notify(bool finish) override;
    void wait();
};

class NaviAsyncUserResult : public NaviUserResult {
public:
    NaviAsyncUserResult(NaviUserResultClosure *closure);
public:
    virtual bool nextData(NaviUserData &data, bool &eof) override;
private:
    virtual void notify(bool finish) override;
    bool processData(NaviUserData &data, bool &eof, ErrorCode &retEc);
private:
    NaviUserResultClosure *_resultClosure;
    std::list<NaviUserData> _resultData;
    bool _resultDataEof;
    atomic64_t _pendingNotify;
    autil::ThreadCond _notifyCond;
};


}
