#include <ha3/service/Session.h>
#include <ha3/service/SessionPool.h>
#include <autil/TimeUtility.h>
#include <sap_eagle/RpcContext.h>

using namespace std;
BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, Session);

void Session::destroy() {
    if (_sessionPool) {
        _sessionPool->put(this);
    } else {
        delete this;
    }
}

int64_t Session::getCurrentTime() {
    return autil::TimeUtility::currentTime();
}

bool Session::beginSession() {
    _sessionMetricsCollectorPtr.reset(
            new monitor::SessionMetricsCollector());
    _sessionMetricsCollectorPtr->setSessionStartTime(_startTime);
    _sessionMetricsCollectorPtr->beginSessionTrigger();
    return true;
}

void Session::endSession() {
    if (!_sessionMetricsCollectorPtr) {
        return;
    }
    if (_pool) {
        _sessionMetricsCollectorPtr->setMemPoolUsedBufferSize(
                _pool->getAllocatedSize());
        _sessionMetricsCollectorPtr->setMemPoolAllocatedBufferSize(
                _pool->getTotalBytes());
    }
    _sessionMetricsCollectorPtr->endSessionTrigger();
}

void Session::reset() {
    _startTime = 0;
    _pool = NULL;
    _sessionMetricsCollectorPtr.reset();
    _gigQuerySession.reset();
    _gigQueryInfo.reset();
}

END_HA3_NAMESPACE(service);
