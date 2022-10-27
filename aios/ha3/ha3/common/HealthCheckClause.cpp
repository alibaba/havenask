#include <ha3/common/HealthCheckClause.h>

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, HealthCheckClause);

HealthCheckClause::HealthCheckClause() { 
    _check = false;
    _checkTimes = 0;
    _recoverTime = 3ll * 60 * 1000 * 1000; // 3min
}

HealthCheckClause::~HealthCheckClause() { 
}

void HealthCheckClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_check);
    dataBuffer.write(_checkTimes);
    dataBuffer.write(_recoverTime);
}

void HealthCheckClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_check);
    dataBuffer.read(_checkTimes);
    dataBuffer.read(_recoverTime);
}

std::string HealthCheckClause::toString() const {
    return _originalString;
}

END_HA3_NAMESPACE(common);

