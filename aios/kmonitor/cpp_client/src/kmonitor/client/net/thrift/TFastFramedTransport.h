/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:49
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_TFASTFRAMEDTRANSPORT_H_
#define KMONITOR_CLIENT_NET_THRIFT_TFASTFRAMEDTRANSPORT_H_

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/net/thrift/TSocket.h"
BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TSocket;

class TFastFramedTransport {
public:
    TFastFramedTransport(TSocket *socket, uint32_t buffer_size = 10485760);
    ~TFastFramedTransport();

    bool IsOpen();
    void Open();

    int32_t ReadAll();
    uint32_t Read(uint8_t *buf, uint32_t len);
    int Write(const uint8_t *buf, uint32_t len);
    void Flush();
    uint32_t ReadEnd();
    uint32_t WriteEnd() { return 0; }

private:
    TFastFramedTransport(const TFastFramedTransport &);
    TFastFramedTransport &operator=(const TFastFramedTransport &);
    void EncodeFrameSize(int frameSize, uint8_t buf[4]);

private:
    TSocket *socket_;
    uint8_t *write_buf_;
    uint32_t buffer_size_;
    uint32_t write_buf_pos_;
    uint8_t *read_buf_;
    uint32_t read_buf_pos_;
    uint32_t read_buf_total_;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_NET_THRIFT_TFASTFRAMEDTRANSPORT_H_
