/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/workflow/AsyncStarter.h"

#include <iosfwd>
#include <memory>
#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace workflow {

BS_LOG_SETUP(workflow, AsyncStarter);

AsyncStarter::AsyncStarter() : _running(false), _maxRetryIntervalTime(30 * 1000 * 1000) {}

AsyncStarter::~AsyncStarter() {}

void AsyncStarter::getMaxRetryIntervalTime()
{
    string param = autil::EnvUtil::getEnv("max_retry_interval");
    int64_t maxRetryIntervalTime = 30 * 1000 * 1000;
    if (!param.empty() && StringUtil::fromString(param, maxRetryIntervalTime)) {
        _maxRetryIntervalTime = maxRetryIntervalTime * 1000 * 1000;
    }
}

void AsyncStarter::asyncStart(const func_type& func)
{
    _running = true;
    _func = func;
    getMaxRetryIntervalTime();
    _startThread =
        autil::Thread::createThread(std::bind(&AsyncStarter::loopStart, this), _name.empty() ? "BsAsyncStart" : _name);
}

void AsyncStarter::stop()
{
    if (!_running) {
        return;
    }
    _running = false;
    if (_startThread) {
        _startThread->join();
    }
}

void AsyncStarter::loopStart()
{
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

}} // namespace build_service::workflow
