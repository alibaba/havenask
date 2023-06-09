/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-04-19 15:49
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include <arpa/inet.h>
#include "kmonitor/client/net/thrift/TSocket.h"
#include "kmonitor/client/net/thrift/TFastFramedTransport.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, TFastFramedTransport);

TFastFramedTransport::TFastFramedTransport(TSocket *socket, uint32_t buffer_size) {
    socket_ = socket;
    buffer_size_ = buffer_size;
    write_buf_ = new uint8_t[buffer_size_];
    read_buf_ = new uint8_t[buffer_size_];
    write_buf_pos_ = sizeof(int32_t);
    read_buf_pos_ = 0;
    read_buf_total_ = 0;
}

TFastFramedTransport::~TFastFramedTransport() {
    delete[] write_buf_;
    delete[] read_buf_;
}

bool TFastFramedTransport::IsOpen() {
    return socket_->IsConnect();
}

void TFastFramedTransport::Open() {
    socket_->Connect();
}

int32_t TFastFramedTransport::ReadAll() {
    // read message size
    uint8_t tmp_buf[4] = {0};
    auto ret = socket_->Read(tmp_buf, 4);
    if (ret <= 0) {
        return ret;
    }
    auto tmp_len = *(reinterpret_cast<uint32_t*>(tmp_buf));
    auto nlen = static_cast<int32_t>(ntohl(tmp_len));
    
    // read message body
    int32_t nread = socket_->Read(read_buf_, nlen);
    if (nread != nlen) {
        return -1;
    }
    read_buf_total_ = nread;
    read_buf_pos_ = 0;
    return nlen;
}

uint32_t TFastFramedTransport::ReadEnd() {
    read_buf_pos_ = 0;
    read_buf_total_ = 0;
    return 0;
}


uint32_t TFastFramedTransport::Read(uint8_t* buf, uint32_t len) {
    if (read_buf_pos_ + len > read_buf_total_) {
        return 0;
    }
    memcpy(buf, read_buf_+read_buf_pos_, len);
    read_buf_pos_ += len;
    return len;
}

int TFastFramedTransport::Write(const uint8_t* buf, uint32_t len) {
    if (write_buf_pos_ + len > buffer_size_) {
        AUTIL_LOG(WARN, "write_buf_pos[%u] + len[%u] larger than write_buf_size[%u]",
                     write_buf_pos_, len, buffer_size_);
        return -1;
    }
    memcpy(write_buf_+write_buf_pos_, buf, len);
    write_buf_pos_ += len;
    return 0;
}

void TFastFramedTransport::Flush() {
    int32_t real_len = write_buf_pos_ - sizeof(int32_t);
    int32_t nlen = (int32_t)htonl((uint32_t)(real_len));
    //yytodo socket write failed, need retrans or drop
    memcpy(write_buf_, (uint8_t*)&nlen, sizeof(nlen));
    socket_->Write((const uint8_t*)write_buf_, write_buf_pos_);
    write_buf_pos_ = sizeof(int32_t);

    // check socket state
    if (socket_->GetSocketState() != CONNECTED) {
        AUTIL_LOG(WARN, "socket write failed, socket is in problem");
        // TODO sleep a litte time?
        socket_->ReConnect();
    }
}

void TFastFramedTransport::EncodeFrameSize(int frame_size, uint8_t buf[4]) {
    buf[0] = (uint8_t)(0xff & (frame_size >> 24));
    buf[1] = (uint8_t)(0xff & (frame_size >> 16));
    buf[2] = (uint8_t)(0xff & (frame_size >> 8));
    buf[3] = (uint8_t)(0xff & (frame_size));
}

END_KMONITOR_NAMESPACE(kmonitor);

