#include <ha3/common/SqlAccessLog.h>

using namespace std;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SqlAccessLog);

SqlAccessLog::SqlAccessLog():_ip("unknownip") {
    _processTime = 0;
    _formatTime = 0;
    _statusCode = ERROR_NONE;
    _rowCount = 0;
}

SqlAccessLog::SqlAccessLog(const std::string& clientIp)
    : _ip(clientIp)
{
    _processTime = 0;
    _formatTime = 0;
    _statusCode = ERROR_NONE;
    _rowCount = 0;
}

SqlAccessLog::~SqlAccessLog() {
    log();
}

void SqlAccessLog::log() {
    if (_processTime >= (int64_t)_logSearchInfoThres) {
        HA3_LOG(INFO, "%s \t %4u \t %4d \t %u \t %ldus \t %ldus \t %s \t %4d \t query=[%s] \t run_info=[%s]",
                _ip.c_str(), _rowCount, _statusCode, _resultLen,
                _processTime, _formatTime,
                _traceId.empty() ? "0" : _traceId.c_str(),
                _sessionSrcType,
                _queryString.c_str(),
                _searchInfo.ShortDebugString().c_str());
    } else {
        HA3_LOG(INFO, "%s \t %4u \t %4d \t %u \t %ldus \t %ldus \t %s \t %4d \t  query=[%s]",
                _ip.c_str(), _rowCount, _statusCode, _resultLen,
                _processTime, _formatTime,
                _traceId.empty() ? "0" : _traceId.c_str(),
                _sessionSrcType,
                _queryString.c_str());
    }
}

END_HA3_NAMESPACE(common);
