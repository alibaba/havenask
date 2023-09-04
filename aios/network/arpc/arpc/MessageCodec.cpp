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
#include "aios/network/arpc/arpc/MessageCodec.h"

#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/RPCMessageSerializable.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "aios/network/arpc/arpc/util/Log.h"

ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(MessageCodec);

MessageCodec::MessageCodec() {}

MessageCodec::~MessageCodec() {}

CallId MessageCodec::GenerateCallId(const RPCMethodDescriptor *method, version_t version) const {
    CallId callId;
    callId.intId = PacketCodeBuilder()(method);
    return callId;
}

ARPC_END_NAMESPACE(arpc);
