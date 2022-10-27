#ifndef ISEARCH_SESSIONPOOL_H
#define ISEARCH_SESSIONPOOL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/Session.h>
#include <anet/atomic.h>
#include <autil/Lock.h>

BEGIN_HA3_NAMESPACE(service);

class SessionPool
{
public:
    const static uint32_t defautMaxSessionSize = 400;
public:
    SessionPool(uint32_t maxSessionSize);
    virtual ~SessionPool();
private:
    SessionPool(const SessionPool&);
    SessionPool& operator = (const SessionPool&);
public:
    virtual Session* get() = 0;
    virtual void put(Session* session) = 0;
    uint32_t getMaxSessionSize() const { return _maxSessionSize; }
    uint32_t getCurrentCount() const { return _sessionCount; }
private: //for test
    void setMaxSessionSize(uint32_t maxSessionSize) {
        _maxSessionSize = maxSessionSize;
    }
protected:
    uint32_t _maxSessionSize;
    uint32_t _sessionCount;
private:
    friend class SessionPoolTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SessionPool);

template<typename SessionType>
class SessionPoolImpl : public SessionPool
{
public:
    SessionPoolImpl(uint32_t maxSessionSize = SessionPool::defautMaxSessionSize)
        : SessionPool(maxSessionSize)
    {
        _sessions = (SessionType*)malloc(sizeof(SessionType) * _maxSessionSize);
        _freeSessions.reserve(_maxSessionSize);
        for (uint32_t i = 0; i < _maxSessionSize; ++i) {
            new (_sessions+i) SessionType(this);
            _freeSessions.push_back(_sessions+i);
        }
    }
    ~SessionPoolImpl() {
        for (uint32_t i = 0; i < _maxSessionSize; ++i) {
            (_sessions+i)->~SessionType();  
        }
        free(_sessions);
    }
public:
    SessionType* get() {
        //the max num of session may be _maxSessionSize + num of active threads
        autil::ScopedLock lock(_mutex);
        if (_sessionCount >= _maxSessionSize) {
            HA3_LOG(WARN, "Allocate Session failed,the num of sessions has reached limit!");
            return NULL;
        }
        _sessionCount++;
        assert(!_freeSessions.empty());
        SessionType* session = _freeSessions.back();
        _freeSessions.pop_back();
        return session;
    }
    void put(Session* session) {
        assert(NULL != session);
        session->reset();
        autil::ScopedLock lock(_mutex);
        _freeSessions.push_back(static_cast<SessionType*>(session));
        _sessionCount--;
    }
private:
    autil::ThreadMutex _mutex;
    SessionType* _sessions;
    std::vector<SessionType*> _freeSessions;
private:
    friend class SessionPoolTest;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP_TEMPLATE(service, SessionPoolImpl, SessionType);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SESSIONPOOL_H
