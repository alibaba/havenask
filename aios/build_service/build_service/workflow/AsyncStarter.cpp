#include "build_service/workflow/AsyncStarter.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, AsyncStarter);

AsyncStarter::AsyncStarter()
    : _running(false)
    , _maxRetryIntervalTime(30 * 1000 * 1000)
{
}

AsyncStarter::~AsyncStarter() {
}

void AsyncStarter::getMaxRetryIntervalTime() {
    const char* param = getenv("max_retry_interval");
    int64_t maxRetryIntervalTime = 30 * 1000 * 1000;
    if (param && StringUtil::fromString(string(param), maxRetryIntervalTime)) {
        _maxRetryIntervalTime = maxRetryIntervalTime * 1000 * 1000;
    }
}

void AsyncStarter::asyncStart(const func_type &func) {
    _running = true;
    _func = func;
    getMaxRetryIntervalTime();
    _startThread = autil::Thread::createThread(std::tr1::bind(&AsyncStarter::loopStart, this),
                                               _name.empty() ? "BsAsyncStart" : _name);
}

void AsyncStarter::stop() {
    if (!_running) {
        return;
    }
    _running = false;
    if (_startThread) {
        _startThread->join();
    }
}

void AsyncStarter::loopStart() {
    uint32_t retryInterval = 1000 * 1000;
    while (_running) {
        if (!_func()) {
            usleep(retryInterval);
            retryInterval *= 2;
            if (retryInterval > _maxRetryIntervalTime) {
                retryInterval = _maxRetryIntervalTime;
            }   
            continue;
        }
        break;
    }
}

}
}
