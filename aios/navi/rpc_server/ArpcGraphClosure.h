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
#pragma once

#include "navi/engine/NaviUserResult.h"
#include <google/protobuf/service.h>

namespace navi {

class ArpcGraphClosure : public NaviUserResultClosure
{
public:
    ArpcGraphClosure(const std::string &method,
                     google::protobuf::RpcController *controller,
                     google::protobuf::Message *response,
                     google::protobuf::Closure *done);
    ~ArpcGraphClosure();
    ArpcGraphClosure(const ArpcGraphClosure &) = delete;
    ArpcGraphClosure &operator=(const ArpcGraphClosure &) = delete;
public:
    void run(NaviUserResultPtr result) override;
private:
    const std::string &_method;
    google::protobuf::RpcController *_controller;
    google::protobuf::Message *_response;
    google::protobuf::Closure *_done;
};

}

