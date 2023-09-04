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

#include "aios/apps/facility/cm2/cm_basic/util/server.h"

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, Server);

bool Server::stop()
{
    autil::ScopedLock lock(_cond);

    if (_status == srv_startting) {
        AUTIL_LOG(WARN, "this server is been startting now, stop latter.");
        return false;
    } else if (_status == srv_stopping) {
        AUTIL_LOG(WARN, "maybe another thread is stopping this server now.");
        return false;
    } else if (_status == srv_stopped) {
        AUTIL_LOG(WARN, "this server has been stopped yet.");
        return true;
    } else if (_status != srv_startted) {
        AUTIL_LOG(WARN, "this server is not been startted yet.");
        return false;
    }
    _status = srv_stopping;
    release();
    _status = srv_stopped;
    _cond.broadcast();

    AUTIL_LOG(INFO, "server(%p) has been stopped.", this);
    return true;
}

bool Server::wait()
{
    autil::ScopedLock lock(_cond);
    while (_status == srv_startting || _status == srv_startted || _status == srv_stopping) {
        _cond.wait();
    }

    if (_status != srv_stopped)
        return false;
    return true;
}

} // namespace cm_basic
