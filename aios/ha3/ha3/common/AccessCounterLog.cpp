#include <ha3/common/AccessCounterLog.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AccessCounterLog);

AccessCounterLog::AccessCounterLog() 
    : _lastLogTime(0)
{ 
}

AccessCounterLog::~AccessCounterLog() { 
}

bool AccessCounterLog::canReport() {
    int64_t now = TimeUtility::currentTimeInSeconds();
    if (now >= _lastLogTime + LOG_INTERVAL) {
        _lastLogTime = now;
        return true;
    } else {
        return false;
    }
    return true;
}

void AccessCounterLog::reportIndexCounter(const string &name, uint32_t count) {
    log("Index", name, count);
}

void AccessCounterLog::reportAttributeCounter(
        const string &name, uint32_t count) 
{
    log("Attribute", name, count);
}

void AccessCounterLog::log(const string &prefix, 
                           const string &name, uint32_t count) 
{
    HA3_LOG(INFO, "[%s: %s]: %u", prefix.c_str(), name.c_str(), count);
}

END_HA3_NAMESPACE(common);

