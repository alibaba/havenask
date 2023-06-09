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
#ifndef NAVI_RPCPARAM_H
#define NAVI_RPCPARAM_H

#include <multi_call/rpc/GigClosure.h>
#include "navi/proto/NaviRpc.pb.h"

namespace navi {

struct RpcParam
{
public:
    RpcParam(google::protobuf::RpcController *controller,
             const NaviRequestDef *request,
             NaviResponseDef *response,
             google::protobuf::Closure *done)
        : _controller(controller)
        , _request(request)
        , _response(response)
        , _done(done)
    {
    }
    ~RpcParam() {
        if (_done) {
            auto gigClosure = dynamic_cast<multi_call::GigClosure *>(_done);
            assert(gigClosure);
            gigClosure->getController().SetFailed("navi run graph failed");
            gigClosure->Run();
        }
    }
    const NaviRequestDef *getRequest() const {
        return _request;
    }
    google::protobuf::Closure *stealClosure() {
        auto ret = _done;
        _done = nullptr;
        return ret;
    }
private:
    RpcParam(const RpcParam &);
    RpcParam &operator=(const RpcParam &);
private:
    google::protobuf::RpcController *_controller;
    const NaviRequestDef *_request;
    NaviResponseDef *_response;
    google::protobuf::Closure *_done;
};

}

#endif //NAVI_RPCPARAM_H
