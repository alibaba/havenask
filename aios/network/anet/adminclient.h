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
#ifndef ADMIN_CLIENT_H
#define ADMIN_CLIENT_H

#include "aios/network/anet/anet.h"

#include "aios/network/anet/defaultpacketstreamer.h"

namespace anet {
class IPacketHandler;
class Transport;
}  // namespace anet

#define MAX_CMD_BUF 1024
namespace anet{

class AdminClient
{
public:
    AdminClient(Transport *transport, char *spec);
    ~AdminClient();
    int start(IPacketHandler *handler, void *args, char *cmd, 
              int paramCount, char **params);

private:
    char *_spec;
    Transport * _transport;
    AdvanceDefaultPacketFactory _factory ;
    DefaultPacketStreamer _streamer;
};


}
#endif
