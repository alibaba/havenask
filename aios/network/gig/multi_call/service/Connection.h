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
#ifndef ISEARCH_MULTI_CALL_CONNECTION_H
#define ISEARCH_MULTI_CALL_CONNECTION_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/service/CallBack.h"
#include "autil/LockFreeThreadPool.h"

namespace autil {
class ThreadPool;
}

namespace multi_call {

class Connection
{
public:
    Connection(const std::string &spec, ProtocolType type, size_t queueSize);
    virtual ~Connection();

private:
    Connection(const Connection &);
    Connection &operator=(const Connection &);

public:
    virtual void post(const RequestPtr &request, const CallBackPtr &callBack) = 0;

public:
    const std::string &getSpec() const {
        return _spec;
    }
    std::string getHost() const {
        return _spec.substr(4); // remove "tcp:"
    }
    std::string getHostIp() const {
        size_t end = _spec.rfind(':');
        return _spec.substr(4, end - 4); // remove "tcp:" and end ":port"
    }
    ProtocolType getType() const {
        return _type;
    }
    void startChildRpc(const RequestPtr &request, const CallBackPtr &callBack) const;
    void setCallBackThreadPool(autil::LockFreeThreadPool *callBackThreadPool);

private:
    static size_t genAllocId();

protected:
    size_t _allocId;
    std::string _spec;
    ProtocolType _type;
    size_t _queueSize;
    autil::LockFreeThreadPool *_callBackThreadPool;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(Connection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONNECTION_H
