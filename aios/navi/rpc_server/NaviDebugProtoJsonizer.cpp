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
#include "navi/rpc_server/NaviDebugProtoJsonizer.h"
#include "navi/proto/GraphVis.pb.h"

namespace navi {

NaviDebugProtoJsonizer::NaviDebugProtoJsonizer(
    const std::shared_ptr<GraphVisDef> &visDef,
    const http_arpc::ProtoJsonizerPtr &realProtoJsonizer)
    : _visDef(visDef)
    , _realProtoJsonizer(realProtoJsonizer)
{
}

NaviDebugProtoJsonizer::~NaviDebugProtoJsonizer() {
}

bool NaviDebugProtoJsonizer::fromJson(const std::string &jsonStr,
                                      google::protobuf::Message *message)
{
    assert(false);
    return false;
}

std::string NaviDebugProtoJsonizer::toJson(const google::protobuf::Message &message) {
    auto response = _realProtoJsonizer->toJson(message);
    if (1 != _visDef->nodes_size()) {
        return response;
    }
    auto nodeDef = _visDef->mutable_nodes(0);
    nodeDef->set_user_result(response);
    return _visDef->SerializeAsString();
}

}

