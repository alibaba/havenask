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
#include "suez/worker/SelfKillerService.h"

#include <sys/types.h>
#include <unistd.h>

#include "arpc/ThreadPoolDescriptor.h"
#include "autil/Log.h"
#include "suez/sdk/RpcServer.h"

using namespace std;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SelfKillerService);

namespace suez {

SelfKillerService::SelfKillerService() {}

SelfKillerService::~SelfKillerService() {}

void SelfKillerService::killSelf(::google::protobuf::RpcController *controller,
                                 const ::suez::KillSelfRequest *request,
                                 ::suez::KillSelfResponse *response,
                                 ::google::protobuf::Closure *done) {
    kill(getpid(), 9);
}

bool SelfKillerService::init(RpcServer *server) {
    arpc::ThreadPoolDescriptor threadPoolDesc("self_killer", 1, 1);
    return server->RegisterService(this, threadPoolDesc);
}
} // namespace suez
