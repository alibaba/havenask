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
#ifndef __SERVER_H_
#define __SERVER_H_

#include "autil/Lock.h"
#include "autil/Log.h"

namespace cm_basic {

enum server_status_t { srv_unknown, srv_startting, srv_startted, srv_stopping, srv_stopped };

class Server
{
public:
    Server() : _status(srv_unknown) {}
    virtual ~Server() {}
    virtual bool start(const char* configFile) = 0;
    virtual bool stop();
    virtual bool wait();

protected:
    virtual void release() = 0;

protected:
    server_status_t _status;
    autil::ThreadCond _cond;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
