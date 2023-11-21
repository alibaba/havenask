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
#include "suez/worker/KMonitorDebugService.h"

#include "autil/ClosureGuard.h"
#include "suez/worker/KMonitorManager.h"

namespace suez {

SnapshotProtoJsonizer::SnapshotProtoJsonizer() {}
SnapshotProtoJsonizer::~SnapshotProtoJsonizer() {}

bool SnapshotProtoJsonizer::fromJson(const std::string &jsonStr, google::protobuf::Message *message) { return true; }

std::string SnapshotProtoJsonizer::toJson(const google::protobuf::Message &message) {
    auto *response = dynamic_cast<const KMonitorSnapshotResponse *>(&message);
    if (!response) {
        return std::string("error response type: ") + std::string(typeid(message).name()) +
               ", message: " + http_arpc::ProtoJsonizer::toJson(message);
    } else {
        return response->assemblyresult();
    }
}

KMonitorDebugService::KMonitorDebugService(KMonitorManager &manager) : _manager(manager) {}
KMonitorDebugService::~KMonitorDebugService() {}

void KMonitorDebugService::snapshot(::google::protobuf::RpcController *controller,
                                    const ::suez::KMonitorSnapshotRequest *request,
                                    ::suez::KMonitorSnapshotResponse *response,
                                    ::google::protobuf::Closure *done) {
    autil::ClosureGuard guard(done);
    response->set_assemblyresult(_manager.manuallySnapshot());
}

} // namespace suez
