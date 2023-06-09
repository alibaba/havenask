/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:32
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_TSOCKET_H_
#define KMONITOR_CLIENT_NET_THRIFT_TSOCKET_H_

#include <string>
#include "kmonitor/client/common/Common.h"
#include "autil/Log.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);


enum SocketState {
    TO_BE_CONNECTING = 1,
    CONNECTING,
    CONNECTED,
    CLOSING,
    CLOSED,
};

class TSocket {
 public:
    TSocket(const std::string& host, int port, int32_t time_out_ms = 100);
    virtual ~TSocket();

    bool IsConnect();
    bool Connect();
    bool ReConnect();
    void Close();
    SocketState GetSocketState() const;

    // virtual for test
    virtual int32_t Read(uint8_t* buf, uint32_t len);
    virtual int32_t Write(const uint8_t* buf, uint32_t len); 

 private:
    TSocket(const TSocket &);
    TSocket& operator=(const TSocket &);

 private:
    static const int kInvalidSocket = -1;

 private:
    std::string host_;
    int port_;
    int32_t time_out_ms_;
    int socket_;
    SocketState socket_state_;
    int send_timeout_;
    int recv_timeout_;
    bool keep_alive_;
    bool linger_on_;
    bool linger_val_;
    bool no_delay_;
    int max_recv_retries_;

 private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_THRIFT_TSOCKET_H_
