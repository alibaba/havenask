/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-06-13 18:01
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/net/thrift/TApplicationException.h"

#include <string>

#include "kmonitor/client/net/thrift/TCompactProtocol.h"
#include "kmonitor/client/net/thrift/ThriftType.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;

TApplicationException::TApplicationException() {}

TApplicationException::~TApplicationException() {}

uint32_t TApplicationException::Read(TCompactProtocol *prot) {
    uint32_t xfer = 0;
    std::string fname;
    TType ftype;
    int16_t fid;

    xfer += prot->ReadStructBegin(fname);
    while (true) {
        xfer += prot->ReadFieldBegin(fname, ftype, fid);
        if (ftype == T_STOP) {
            break;
        }
        switch (fid) {
        case 1:
            if (ftype == T_STRING) {
                xfer += prot->ReadString(message_);
            } else {
                xfer += prot->Skip(ftype);
            }
            break;
        case 2:
            if (ftype == T_I32) {
                int32_t type;
                xfer += prot->ReadI32(type);
                type_ = (TApplicationExceptionType)type;
            } else {
                xfer += prot->Skip(ftype);
            }
            break;
        default:
            xfer += prot->Skip(ftype);
            break;
        }
        xfer += prot->ReadFieldEnd();
    }

    xfer += prot->ReadStructEnd();
    return xfer;
}

const char *TApplicationException::What() const {
    if (message_.empty()) {
        switch (type_) {
        case UNKNOWN:
            return "TApplicationException: Unknown application exception";
        case UNKNOWN_METHOD:
            return "TApplicationException: Unknown method";
        case INVALID_MESSAGE_TYPE:
            return "TApplicationException: Invalid message type";
        case WRONG_METHOD_NAME:
            return "TApplicationException: Wrong method name";
        case BAD_SEQUENCE_ID:
            return "TApplicationException: Bad sequence identifier";
        case MISSING_RESULT:
            return "TApplicationException: Missing result";
        case INTERNAL_ERROR:
            return "TApplicationException: Internal error";
        case PROTOCOL_ERROR:
            return "TApplicationException: Protocol error";
        case INVALID_TRANSFORM:
            return "TApplicationException: Invalid transform";
        case INVALID_PROTOCOL:
            return "TApplicationException: Invalid protocol";
        case UNSUPPORTED_CLIENT_TYPE:
            return "TApplicationException: Unsupported client type";
        default:
            return "TApplicationException: (Invalid exception type)";
        }
    } else {
        return message_.c_str();
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
