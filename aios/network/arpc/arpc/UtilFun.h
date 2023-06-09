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
#ifndef ARPC_UTIL_FUN_H
#define ARPC_UTIL_FUN_H
#include <string>
#include <memory>

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageSerializable.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"
#include "aios/network/arpc/arpc/ANetRPCController.h"

ARPC_BEGIN_NAMESPACE(arpc);

ErrorMsg *BuildErrorMsg(const std::string &errMsg, ErrorCode errCode,
                        const std::shared_ptr<google::protobuf::Arena> &arena);

anet::DelayDecodePacket *BuildPacket(anet::PacketHeader *header,
                                     MessageSerializable *pSeri,
                                     bool ownSeri = true);

void DecodePacket(ANetRPCController *pController,
                  anet::DelayDecodePacket *pPacket,
                  RPCMessage *message,
                  const std::shared_ptr<google::protobuf::Arena> &arenaPtr,
                  bool bResponse = true);

ARPC_END_NAMESPACE(arpc);

#endif //ARPC_UTIL_FUN_H
