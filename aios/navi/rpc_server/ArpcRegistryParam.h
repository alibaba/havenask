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

#include "arpc/RPCServer.h"
#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "multi_call/common/common.h"
#include "navi/engine/RunGraphParams.h"
#include "navi/proto/GraphDef.pb.h"

namespace navi {

struct RegistryArpcParam {
    std::string method;
    multi_call::CompatibleFieldInfo compatibleInfo;
    arpc::ThreadPoolDescriptor threadPoolDescriptor;
    http_arpc::ProtoJsonizerPtr protoJsonizer;
    std::vector<std::string> httpAliasVec;
};

struct RegistryGraphParam {
    std::string requestDataName;
    RunGraphParams params;
    GraphDef graph;
};

struct ArpcRegistryParam {
    RegistryArpcParam arpcParam;
    RegistryGraphParam graphParam;
};

}

