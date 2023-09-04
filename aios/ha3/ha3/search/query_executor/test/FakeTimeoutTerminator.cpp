#include "ha3/search/test/FakeTimeoutTerminator.h"

#include "autil/Log.h"
#include "autil/TimeoutTerminator.h"

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FakeTimeoutTerminator);

FakeTimeoutTerminator::FakeTimeoutTerminator(int timeOutCount)
    : TimeoutTerminator(10, 0) {
    _timeOutCount = timeOutCount;
    _curCount = 0;
}

FakeTimeoutTerminator::~FakeTimeoutTerminator() {}

int64_t FakeTimeoutTerminator::getCurrentTime() {
    if (++_curCount >= _timeOutCount) {
        // return timeout
        return 100;
    } else {
        // return not timeout
        return 0;
    }
}

} // namespace search
} // namespace isearch
