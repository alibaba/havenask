/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:32
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/net/thrift/TSocket.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "autil/Log.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, TSocket);

using std::string;
const int TSocket::kInvalidSocket;

TSocket::TSocket(const string &host, int port, int32_t time_out_ms) {
    host_ = host;
    port_ = port;
    socket_ = kInvalidSocket;
    socket_state_ = TO_BE_CONNECTING;
    send_timeout_ = 0;
    recv_timeout_ = 0;
    keep_alive_ = false;
    linger_on_ = 1;
    linger_val_ = 0;
    no_delay_ = 1;
    max_recv_retries_ = 5;
    time_out_ms_ = time_out_ms;
}

TSocket::~TSocket() { Close(); }

bool TSocket::IsConnect() { return socket_ != kInvalidSocket; }

bool TSocket::Connect() {
    socket_ = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_ < 0) {
        AUTIL_LOG(ERROR, "create socket failed");
        return false;
    }

    // handle close socket
    signal(SIGPIPE, SIG_IGN);

    // set socket opt
    struct timeval ti;
    ti.tv_sec = 0;
    ti.tv_usec = time_out_ms_ * 1000;
    setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &ti, sizeof(ti));
    setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &ti, sizeof(ti));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = inet_addr(host_.c_str());
    serv_addr.sin_port = htons(port_);

    socket_state_ = CONNECTING;
    if (connect(socket_, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        AUTIL_LOG(WARN, "socket connect host[%s] port[%d] failed", host_.c_str(), port_);
        return false;
    }
    socket_state_ = CONNECTED;
    return true;
}

bool TSocket::ReConnect() {
    Close();
    return Connect();
}

void TSocket::Close() {
    if (socket_state_ != CLOSED && socket_ != kInvalidSocket) {
        ::close(socket_);
        socket_ = kInvalidSocket;
        socket_state_ = CLOSED;
    }
}

SocketState TSocket::GetSocketState() const { return socket_state_; }

int32_t TSocket::Read(uint8_t *buf, uint32_t count) {
    int32_t nread = 0;
    uint32_t totlen = 0;
    while (totlen != count) {
        nread = ::read(socket_, (void *)buf, count - totlen);
        if (nread == 0) {
            AUTIL_LOG(WARN, "no data read in socket");
            return totlen;
        }
        if (nread == -1) {
            AUTIL_LOG(WARN, "read failed in socket: errno=%d", errno);
            return -1;
        }
        totlen += nread;
        buf += nread;
    }
    return totlen;
}

int32_t TSocket::Write(const uint8_t *buf, uint32_t count) {
    if (CONNECTED == socket_state_) {
        ssize_t nwritten, totlen = 0;
        while (totlen != count) {
            nwritten = ::write(socket_, buf, count - totlen);
            if (nwritten == 0)
                return totlen;
            if (nwritten == -1) {
                AUTIL_LOG(WARN, "write failed in socket: errno=%d", errno);
                socket_state_ = CLOSING;
                return -1;
            }
            totlen += nwritten;
            buf += nwritten;
        }
        return totlen;
    } else {
        AUTIL_LOG(WARN, "socket is unnormal, can't write socket");
        return -1;
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
