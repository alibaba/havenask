#include <ha3/common/AccessLog.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, AccessLog);

AccessLog::AccessLog():_ip("unknownip") {
    _processTime = 0;
    _statusCode = ERROR_NONE;
    _totalHits = 0;
    _rowKey = 0;
}

AccessLog::AccessLog(const std::string& clientIp)
    : _ip(clientIp)
{
    _processTime = 0;
    _statusCode = ERROR_NONE;
    _totalHits = 0;
    _rowKey = 0;
}

AccessLog::~AccessLog() {
    log();
}

void AccessLog::setPhaseOneSearchInfo(const PhaseOneSearchInfo &info) {
    _searchInfo = info;
}

void AccessLog::log() {
    HA3_LOG(INFO, "%s %4u %lu %4d %ldms %s eagle=[%d %s %s] query=[%s]",
            _ip.c_str(), _totalHits, _rowKey, _statusCode,
            _processTime, _searchInfo.toString().c_str(),
            _sessionSrcType,
            _traceId.empty() ? "0" : _traceId.c_str(),
            _userData.empty() ? "0" : _userData.c_str(),
            _queryString.c_str());
}


END_HA3_NAMESPACE(common);
