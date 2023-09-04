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

#include "suez/worker/KillSelf.pb.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google

namespace suez {
class RpcServer;

class SelfKillerService : public SelfKiller {
public:
    SelfKillerService();
    ~SelfKillerService();

private:
    SelfKillerService(const SelfKillerService &);
    SelfKillerService &operator=(const SelfKillerService &);

public:
    void killSelf(::google::protobuf::RpcController *controller,
                  const ::suez::KillSelfRequest *request,
                  ::suez::KillSelfResponse *response,
                  ::google::protobuf::Closure *done) override;

public:
    bool init(RpcServer *server);
};

} // namespace suez
