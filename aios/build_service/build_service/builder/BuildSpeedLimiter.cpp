#include "build_service/builder/BuildSpeedLimiter.h"
#include <autil/TimeUtility.h>

using namespace autil;

namespace build_service {
namespace builder {

BS_LOG_SETUP(builder, BuildSpeedLimiter);

#define LOG_PREFIX _buildIdStr.c_str()

BuildSpeedLimiter::BuildSpeedLimiter(const std::string &buildIdStr)
    : _buildIdStr(buildIdStr)
    , _targetQps(0) 
    , _counter(0)
    , _lastCheckTime(0)
    , _sleepPerDoc(0)
    , _enableLimiter(false)
{ 
}

BuildSpeedLimiter::~BuildSpeedLimiter() { 
}

void BuildSpeedLimiter::setTargetQps(int32_t sleepPerDoc, uint32_t buildQps) {
    if (_targetQps != buildQps) {
        BS_PREFIX_LOG(INFO, "changing buildQps from %u to %u", _targetQps, buildQps);
        _targetQps = buildQps; 
    }
    if (_sleepPerDoc != sleepPerDoc) {
        BS_PREFIX_LOG(INFO, "changing sleepPerDoc from %d to %d", _sleepPerDoc, sleepPerDoc);
        _sleepPerDoc = sleepPerDoc;
    }
}

void BuildSpeedLimiter::limitSpeed() {
    if (unlikely(_sleepPerDoc)) {
        sleep(_sleepPerDoc);
    }
    if (!_targetQps || !_enableLimiter) {
        return;
    }
    if (_counter == 0) {
        _lastCheckTime = TimeUtility::currentTime();
        ++_counter;
        return;
    }
    if (++_counter < _targetQps) {
        return;
    }
    _counter = 0;
    int64_t now = TimeUtility::currentTime();
    int64_t delta = now - _lastCheckTime;
    if (delta < ONE_SECOND) {
        usleep(ONE_SECOND - delta);
    } else {
        BS_PREFIX_LOG(DEBUG, "reading %u documents used more than 1 second", _targetQps);
    }
}

#undef LOG_PREFIX

}
}

