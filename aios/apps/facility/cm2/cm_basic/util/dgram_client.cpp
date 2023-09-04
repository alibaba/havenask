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
#include "aios/apps/facility/cm2/cm_basic/util/dgram_client.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

namespace cm_basic {

CDGramClient::CDGramClient() { m_sock = -1; }

CDGramClient::~CDGramClient()
{
    if (m_sock != -1) {
        close(m_sock);
        m_sock = -1;
    }
}

int32_t CDGramClient::InitDGramClient(const char* pIp, uint16_t nPort)
{
    if (m_sock != -1)
        close(m_sock);

    /* get a datagram socket */
    if ((m_sock = m_cDGramComm.make_dgram_client_socket()) < 0) {
        return -1;
    }

    /* combine hostname and portnumber of destination into an address */
    memset(&m_saddr, 0, sizeof(m_saddr));
    if (m_cDGramComm.make_internet_address(pIp, nPort, &m_saddr) < 0) {
        return -1;
    }

    return 0;
}

/* send a string through the socket to that address */
int CDGramClient::send(const char* pMsg, int nLen)
{
    return sendto(m_sock, pMsg, nLen, 0, (struct sockaddr*)&m_saddr, sizeof(m_saddr));
}

} // namespace cm_basic
