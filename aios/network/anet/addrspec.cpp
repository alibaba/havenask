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
#include "aios/network/anet/addrspec.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "aios/network/anet/ilogger.h"
#include "aios/network/anet/log.h"

#define UNIX_PATH_MAX 108

namespace anet {

bool AddrSpec::isIpAddrStr(const char *str) {
    if (str == NULL)
        return false;
    bool isIPAddr = true;
    char c = '\0';
    const char *p = str;

    while ((c = (*p++)) != '\0') {
        if ((c != '.') && (!((c >= '0') && (c <= '9')))) {
            isIPAddr = false;
            break;
        }
    }
    return isIPAddr;
}

int AddrSpec::getIpByHost(const char *address, struct sockaddr_in *in) {
    if (address == NULL || in == NULL)
        return -1;

    int rc = 0;
    struct addrinfo hint;
    struct addrinfo *res;
    memset(&hint, 0, sizeof(hint));
    hint.ai_family = AF_INET;

    rc = getaddrinfo(address, NULL, &hint, &res);

    if (rc == 0) {
        in->sin_addr.s_addr = ((struct sockaddr_in *)(res->ai_addr))->sin_addr.s_addr;
        /* in either case, we should free memory allocated by glibc. */
        freeaddrinfo(res);
    } else {
        /* error processing */
        ANET_LOG(ERROR,
                 "getaddrinfo failed for host:%s ,error code:%d, "
                 "err str:%s\n",
                 address,
                 rc,
                 gai_strerror(rc));
    }
    return rc;
}

int AddrSpec::setInetAddr(int protocolType, const char *address, int port) {
    int rc = 0;
    if (address == NULL || address[0] == '\0') {
        _addr.in.sin_addr.s_addr = htonl(INADDR_ANY);
    } else {
        if (isIpAddrStr(address)) {
            _addr.in.sin_addr.s_addr = inet_addr(address);
        } else {
            rc = getIpByHost(address, &(_addr.in));
        }
    }
    _addr.in.sin_port = htons(static_cast<short>(port));
    _addr.in.sin_family = AF_INET;
    _type = protocolType;

    _isValid = !rc;
    return rc;
}

int AddrSpec::setUnixAddr(int protocolType, const char *unixPath) {
    if (unixPath == NULL) {
        _isValid = false;
        return -1;
    }

    _addr.un.sun_family = AF_UNIX;
    strncpy(_addr.un.sun_path, unixPath, UNIX_PATH_MAX);

    /* TODO: shall we verify if the unix file is there already ? */
    _type = protocolType;
    _isValid = true;

    return 0;
}

int AddrSpec::setAddrSpec(const char *spec) {
    if (spec == NULL)
        return -1;

    char tmp[512];
    char *args[32];
    strncpy(tmp, spec, 512);
    tmp[511] = '\0';

    int num = parseSpec(tmp, args, 32);

    if (num > 3 || num < 2) {
        return -1;
    }

    if (strcasecmp(args[0], "tcp") == 0 && num == 3) {
        if (args[2] == NULL)
            return -1;
        char *host = args[1];
        while (host != NULL && *host == '/')
            host++;
        int port = atoi(args[2]);
        return setInetAddr(SOCK_STREAM, host, port);

    } else if (strcasecmp(args[0], "udp") == 0 && num == 3) {
        if (args[2] == NULL)
            return -1;
        char *host = args[1];
        while (host != NULL && *host == '/')
            host++;
        int port = atoi(args[2]);
        return setInetAddr(SOCK_DGRAM, host, port);

    } else if (strcasecmp(args[0], "unixstream") == 0 && num == 2) {
        if (args[1] == NULL)
            return -1;
        char *unixPath = args[1];
        return setUnixAddr(SOCK_STREAM, unixPath);

    } else if (strcasecmp(args[0], "unixdgram") == 0 && num == 2) {
        if (args[1] == NULL)
            return -1;
        char *unixPath = args[1];
        return setUnixAddr(SOCK_DGRAM, unixPath);
    } else {
        /* invalid spec string */
        ANET_LOG(ERROR, "Invalid spec string: %s", spec);
        return -2;
    }
}

int AddrSpec::parseSpec(char *src, char **args, int cnt) {
    if (src == NULL)
        return 0;
    int index = 0;
    char *prev = src;

    while (*src) {
        if (*src == ':') {
            *src = '\0';

            if (src != prev)
                args[index++] = prev;
            else
                args[index++] = NULL;

            if (index >= cnt) {
                return index;
            }
            prev = src + 1;
        }
        src++;
    }
    if (*prev)
        args[index++] = prev;
    return index;
}

} // namespace anet
