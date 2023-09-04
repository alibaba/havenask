/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-05-11 15:24
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/net/thrift/ThriftProtocolClient.h"

#include <vector>

#include "autil/Log.h"
#include "kmonitor/client/net/thrift/TApplicationException.h"
#include "kmonitor/client/net/thrift/TCompactProtocol.h"
#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"
#include "kmonitor/client/net/thrift/ThriftType.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, ThriftProtocolClient);

using std::vector;

ThriftProtocolClient::ThriftProtocolClient(TCompactProtocol *compact_protocol, uint32_t perBatchMaxEvents) {
    compact_protocol_ = compact_protocol;
    perBatchMaxEvents_ = perBatchMaxEvents;

    if (perBatchMaxEvents_ <= 0 || perBatchMaxEvents_ > 100000) {
        perBatchMaxEvents_ = 1000;
    }
}

ThriftProtocolClient::~ThriftProtocolClient() {}

/*
Status::type ThriftProtocolClient::Append(const ThriftFlumeEvent& event) {
    send_append(event);
    return RecvBatch();
}
*/

Status::type ThriftProtocolClient::AppendBatch(const vector<ThriftFlumeEvent *> &events) {
    SendBatch(events);
    return RecvBatch();
}

void ThriftProtocolClient::SendBatch(const vector<ThriftFlumeEvent *> &events) {
    int32_t cseqid = 0;
    uint32_t xfer = 0;
    uint32_t loopTime = 0;
    uint32_t total_events_size = events.size();
    while (loopTime < total_events_size) {
        if (loopTime % perBatchMaxEvents_ == 0) {
            uint32_t cur_events_size = perBatchMaxEvents_;
            if (total_events_size - loopTime > perBatchMaxEvents_) {
                cur_events_size = perBatchMaxEvents_;
            } else {
                cur_events_size = total_events_size - loopTime;
            }

            xfer = compact_protocol_->WriteMessageBegin("appendBatch", T_CALL, cseqid);
            xfer += compact_protocol_->WriteStructBegin("ThriftSourceProtocol_appendBatch_pargs");
            xfer += compact_protocol_->WriteFieldBegin("events", T_LIST, 1);
            xfer += compact_protocol_->WriteListBegin(T_STRUCT, cur_events_size);
        }

        xfer += events[loopTime]->Write(compact_protocol_);

        ++loopTime;
        if ((loopTime % perBatchMaxEvents_ == 0) || (loopTime >= total_events_size)) {
            AUTIL_LOG(DEBUG, "[%u] events set to this batch", loopTime);
            xfer += compact_protocol_->WriteListEnd();
            xfer += compact_protocol_->WriteFieldEnd();
            xfer += compact_protocol_->WriteFieldStop();
            xfer += compact_protocol_->WriteStructEnd();
            compact_protocol_->WriteMessageEnd();
            compact_protocol_->GetTransport()->WriteEnd();
            compact_protocol_->GetTransport()->Flush();
        }
    }
}

Status::type ThriftProtocolClient::RecvBatch() {
    int32_t rseqid = 0;
    std::string fname;
    TMessageType mtype;

    if (compact_protocol_->ReadMessageBegin(fname, mtype, rseqid) == 0) {
        compact_protocol_->ReadMessageEnd();
        compact_protocol_->GetTransport()->ReadEnd();
        return Status::ERROR;
    }
    if (mtype == T_EXCEPTION) {
        TApplicationException exception;
        exception.Read(compact_protocol_);
        compact_protocol_->ReadMessageEnd();
        compact_protocol_->GetTransport()->ReadEnd();
        AUTIL_LOG(WARN, "send metrics failed:%s", exception.What());
        return Status::FAILED;
    }
    if (mtype != T_REPLY) {
        compact_protocol_->Skip(T_STRUCT);
        compact_protocol_->ReadMessageEnd();
        compact_protocol_->GetTransport()->ReadEnd();
        AUTIL_LOG(WARN, "thrift result not reply, mtype=%d", mtype);
        return Status::FAILED;
    }
    if (fname.compare("appendBatch") != 0) {
        compact_protocol_->Skip(T_STRUCT);
        compact_protocol_->ReadMessageEnd();
        compact_protocol_->GetTransport()->ReadEnd();
        AUTIL_LOG(WARN, "thrift result not appendBatch");
        return Status::FAILED;
    }

    uint32_t xfer = 0;
    TType ftype;
    int16_t fid;
    xfer += compact_protocol_->ReadStructBegin(fname);

    Status::type ret = Status::OK;
    bool flag = false;
    while (true) {
        xfer += compact_protocol_->ReadFieldBegin(fname, ftype, fid);
        if (ftype == T_STOP) {
            break;
        }
        switch (fid) {
        case 0:
            if (ftype == T_I32) {
                int32_t ecast;
                xfer += compact_protocol_->ReadI32(ecast);
                ret = (Status::type)ecast;
                flag = true;
            } else {
                xfer += compact_protocol_->Skip(ftype);
            }
            break;
        default:
            xfer += compact_protocol_->Skip(ftype);
            break;
        }
        xfer += compact_protocol_->ReadFieldEnd();
    }
    xfer += compact_protocol_->ReadStructEnd();

    compact_protocol_->ReadMessageEnd();
    compact_protocol_->GetTransport()->ReadEnd();

    if (flag) {
        if (ret == Status::OK) {
            AUTIL_LOG(DEBUG, "send appendBatch metric success");
        }
    } else {
        AUTIL_LOG(WARN, "send appendBatch metric failed");
        return Status::UNKNOWN;
    }
    return ret;
}

END_KMONITOR_NAMESPACE(kmonitor);
