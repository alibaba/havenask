#include <ha3/search/test/FakeTimeoutTerminator.h>

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, FakeTimeoutTerminator);

FakeTimeoutTerminator::FakeTimeoutTerminator(int timeOutCount)
    : TimeoutTerminator(10, 0)
{ 
    _timeOutCount = timeOutCount;
    _curCount = 0;
}

FakeTimeoutTerminator::~FakeTimeoutTerminator() { 
}

int64_t FakeTimeoutTerminator::getCurrentTime() {
    if (++_curCount >= _timeOutCount) {
        // return timeout
        return 100;
    } else {
        // return not timeout
        return 0;
    }
}

END_HA3_NAMESPACE(search);
