#include <ha3/common/SearcherAccessLog.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SearcherAccessLog);

SearcherAccessLog::SearcherAccessLog()
    : _entryTime(TimeUtility::currentTime())
{
}

SearcherAccessLog::~SearcherAccessLog() {
    _exitTime = TimeUtility::currentTime();
    log();
}

void SearcherAccessLog::log() {
    HA3_LOG(INFO, "%ld->%ld %ldus {{{ %s }}}",
            _entryTime, _exitTime, _exitTime - _entryTime, _payload.c_str());
}

END_HA3_NAMESPACE(common);
