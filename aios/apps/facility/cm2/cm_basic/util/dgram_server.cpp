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
#include "aios/apps/facility/cm2/cm_basic/util/dgram_server.h"

#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

namespace cm_basic {

CDGramServer::CDGramServer() { m_sock = -1; }

CDGramServer::~CDGramServer()
{
    if (m_sock != -1) {
        // shutdown(m_sock, 2);
        close(m_sock);
        m_sock = -1;
    }
}

int32_t CDGramServer::InitDGramServer(int nPort)
{
    /* get a datagram socket */
    if ((m_sock = m_cDGramComm.make_dgram_server_socket(nPort)) < 0) {
        return -1;
    }

    return 0;
}

/* receive messaages on the socket(m_sock) */
int32_t CDGramServer::receive(char* buf, int buf_size)
{
    sockaddr_in saddr; /* put sender's address here	*/
    socklen_t saddrlen = sizeof(saddr);

    int32_t nLen = recvfrom(m_sock, buf, buf_size, 0, (sockaddr*)&saddr, &saddrlen);

    return nLen;
}

} // namespace cm_basic
