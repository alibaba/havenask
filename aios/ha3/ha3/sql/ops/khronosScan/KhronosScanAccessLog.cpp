#include <ha3/sql/ops/khronosScan/KhronosScanAccessLog.h>
#include <autil/TimeUtility.h>
#include <sstream>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(sql);

KhronosScanAccessLog::KhronosScanAccessLog()
    : _statusCode(KAL_STATUS_UNKNOWN)
    , _timeoutQuota(-1)
    , _initTime(0)
    , _scanTime(0)
    , _kernelPoolSize(0)
    , _queryPoolSize(0)
{
}

KhronosScanAccessLog::~KhronosScanAccessLog() {
    ostringstream oss;
#define WRITE_FIELDS(key, value, unit)                  \
    oss << key << "=" << (value) << unit  << " "
    WRITE_FIELDS("status", _statusCode, "");
    WRITE_FIELDS("timeoutQuota", _timeoutQuota, "us");
    WRITE_FIELDS("totalTime", _scanTime + _initTime, "us");
    WRITE_FIELDS("initTime", _initTime, "us");
    WRITE_FIELDS("scanTime", _scanTime, "us");
    WRITE_FIELDS("kernelPool", _kernelPoolSize, "bytes");
    WRITE_FIELDS("queryPool", _queryPoolSize, "bytes");
    WRITE_FIELDS("query", "[" + _queryString + "]", "");
#undef WRITE_FIELDS
    _searcherAccessLog.setPayload(oss.str());
}

END_HA3_NAMESPACE(sql);
