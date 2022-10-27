#ifndef ISEARCH_SESSION_H
#define ISEARCH_SESSION_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <sap_eagle/RpcContext.h>
#include <cava/runtime/CavaCtx.h>
#include <multi_call/interface/QuerySession.h>
#include <multi_call/agent/QueryInfo.h>
#include <arpc/CommonMacros.h>
#include <arpc/RPCServerClosure.h>

BEGIN_HA3_NAMESPACE(service);

class SessionPool;

class Session
{
public:
    Session(SessionPool *pool)
        : _sessionPool(pool)
        , _startTime(0)
        , _sessionSrcType(SESSION_SRC_UNKNOWN)
        , _useGigSrc(false)
        , _pool(NULL)
    {
    }
    virtual ~Session() {}
private:
    Session(const Session &);
    Session& operator = (const Session &);
public:
    // either processRequest or dropRequest should be called after create
    virtual void processRequest() = 0;
    virtual void dropRequest() = 0;
    virtual bool beginSession();
    virtual void endSession();
    virtual void reset();
public:
    void setStartTime(int64_t t = getCurrentTime()) {
        _startTime = t;
    };
    int64_t getStartTime() const {
        return _startTime;
    };
    void setSessionSrcType(SessionSrcType type) {
        _sessionSrcType = type;
    }
    SessionSrcType getSessionSrcType() {
        return _sessionSrcType;
    }
    void setGigQuerySession(const multi_call::QuerySessionPtr &querySession) {
        _gigQuerySession = querySession;
    }
    multi_call::QuerySessionPtr getGigQuerySession() {
        return _gigQuerySession;
    }
    void setUseGigSrc(bool flag) {
        _useGigSrc = flag;
    }
public:
    static int64_t getCurrentTime();
    autil::mem_pool::Pool *getMemPool() { return _pool; }
public:
    //for test
    void setMemPool(autil::mem_pool::Pool* pool) {_pool = pool;}
protected:
    // can only be destroyed by sub-class
    void destroy();
protected:
    // use to return self back
    SessionPool *_sessionPool;
    int64_t _startTime;
    SessionSrcType _sessionSrcType;
    bool _useGigSrc = false;
    autil::mem_pool::Pool *_pool;
    monitor::SessionMetricsCollectorPtr _sessionMetricsCollectorPtr;
    multi_call::QuerySessionPtr _gigQuerySession;
    multi_call::QueryInfoPtr _gigQueryInfo;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(Session);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SESSION_H
