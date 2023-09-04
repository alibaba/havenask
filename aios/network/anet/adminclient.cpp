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
#include "aios/network/anet/adminclient.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aios/network/anet/anet.h"
#include "aios/network/anet/connection.h"
#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"
#include "aios/network/anet/transport.h"

namespace anet {
class IPacketHandler;
} // namespace anet

BEGIN_ANET_NS();

AdminClient::AdminClient(Transport *trans, char *spec) : _streamer(&_factory) {
    _spec = strdup(spec);
    _transport = trans;
}

AdminClient::~AdminClient() {
    if (_spec) {
        free(_spec);
    }
}

int AdminClient::start(IPacketHandler *handler, void *args, char *cmd, int paramCount, char **params) {
    Connection *conn;
    conn = _transport->connect(_spec, &_streamer, true);
    if (conn == NULL) {
        ANET_LOG(ERROR, "connection error.");
        return -1;
    }
    conn->setDefaultPacketHandler(handler);
    conn->setQueueLimit(5u);

    _transport->start();
    char buffer[MAX_CMD_BUF];

    /* Now build the admin command buffer.
     * The cmd buffer will contain the following pattern:
     * <cmd str> <param 1> <param 2> ... <param n>
     */
    char paraStr[MAX_CMD_BUF];
    int offset = 0;
    if (paramCount > 0) {
        for (int i = 0; i < paramCount; i++) {
            offset += snprintf(paraStr + offset, MAX_CMD_BUF - offset, " %s", params[i]);
        }
        paraStr[MAX_CMD_BUF - 1] = '\0';
    } else if (paramCount == 0) {
        paraStr[0] = '\0';
    }
    snprintf(buffer, MAX_CMD_BUF, "%s %s", cmd, paraStr);
    buffer[MAX_CMD_BUF - 1] = '\0';

    AdvanceDefaultPacket *packet = new AdvanceDefaultPacket();
    /* The body should include \0 */
    packet->setBody(buffer, strlen(buffer) + 1);
    ANET_LOG(DEBUG, "About to post command to server. \n cmd: %s", buffer);
    if (!(conn->postPacket(packet, handler, args, true))) {
        ANET_LOG(SPAM, "postPacket() Failed! IOC(%p)", conn);
        conn->subRef();
        delete packet;
        return -1;
    }

    ANET_LOG(INFO, "post finish.");
    conn->subRef();

    return 0;
}

END_ANET_NS();
