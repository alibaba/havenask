#include <ha3/service/SessionPool.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, SessionPool);

SessionPool::SessionPool(uint32_t maxSessionSize) { 
    _sessionCount = 0;
    _maxSessionSize = maxSessionSize;
}

SessionPool::~SessionPool() {
}

END_HA3_NAMESPACE(service);
