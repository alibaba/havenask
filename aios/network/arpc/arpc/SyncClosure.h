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
#ifndef ARPC_SYNCCLOSURE_H
#define ARPC_SYNCCLOSURE_H
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/util/Log.h"
#include "aios/network/anet/anet.h"
#include "aios/network/anet/threadcond.h"
#include "aios/network/anet/threadmutex.h"

ARPC_BEGIN_NAMESPACE(arpc);

class SyncClosure : public RPCClosure
{
public:
    SyncClosure()
    {
        _needWait = true;
    }
    virtual ~SyncClosure() {}

public:
    virtual void Run()
    {
        anet::MutexGuard guard(&_cond);
        _needWait = false;
        _cond.signal();
    }

    void WaitReply()
    {
        _cond.lock();

        while (_needWait) {
            _cond.wait();
        }

        _cond.unlock();
    }

private:
    anet::ThreadCond _cond;
    bool _needWait;
};

ARPC_END_NAMESPACE(arpc);

#endif //ARPC_SYNCCLOSURE_H
