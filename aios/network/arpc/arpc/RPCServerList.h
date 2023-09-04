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
#ifndef ARPC_RPCSERVICELIST_H
#define ARPC_RPCSERVICELIST_H
#include <sstream>

#include "aios/autil/autil/Lock.h"
#include "aios/network/arpc/arpc/CommonMacros.h"

ARPC_BEGIN_NAMESPACE(arpc);

class RPCServer;

const int MAX_RPCSERVERS = 1024;

typedef RPCServer *RPCServerListT[MAX_RPCSERVERS];

RPCServerListT &getRPCServerList();
autil::ThreadMutex &getRPCServerListMutex();

bool addRPCServerToList(RPCServer *);
bool delRPCServerFromList(RPCServer *);
int findRPCServerFromList(RPCServer *);
int getRPCServerCount(void);

int dumpRPCServerList(std::ostringstream &out);

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPCSERVICELIST_H
