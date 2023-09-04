/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-05-11 15:26
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"

#include <map>
#include <sstream>
#include <string>

#include "kmonitor/client/net/thrift/TCompactProtocol.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::map;
using std::string;
using std::stringstream;

void ThriftFlumeEvent::SetHeader(const std::map<std::string, std::string> &val) { headers_ = val; }

void ThriftFlumeEvent::SetBody(const std::string &val) { body_ = val; }
const std::string &ThriftFlumeEvent::GetBody() const { return body_; }

uint32_t ThriftFlumeEvent::Write(TCompactProtocol *oprot) const {
    uint32_t xfer = 0;
    xfer += oprot->WriteStructBegin("ThriftFlumeEvent");
    xfer += oprot->WriteFieldBegin("headers", T_MAP, 1);
    {
        xfer += oprot->WriteMapBegin(T_STRING, T_STRING, static_cast<uint32_t>(headers_.size()));
        std::map<std::string, std::string>::const_iterator iter = headers_.begin();
        for (; iter != headers_.end(); iter++) {
            xfer += oprot->WriteString(iter->first);
            xfer += oprot->WriteString(iter->second);
        }
        xfer += oprot->WriteMapEnd();
    }
    xfer += oprot->WriteFieldEnd();
    xfer += oprot->WriteFieldBegin("body", T_STRING, 2);
    xfer += oprot->WriteBinary(body_);
    xfer += oprot->WriteFieldEnd();

    xfer += oprot->WriteFieldStop();
    xfer += oprot->WriteStructEnd();
    return xfer;
}

const map<string, string> &ThriftFlumeEvent::GetHeader() const { return headers_; }

void ThriftFlumeEvent::AddHeader(const std::string &key, const std::string &value) { headers_.emplace(key, value); }

void ThriftFlumeEvent::AddHeaders(const map<std::string, std::string> &headers) {
    headers_.insert(headers.begin(), headers.end());
}

string ThriftFlumeEvent::ToString() const {
    stringstream ss;
    ss << "header:";
    map<string, string>::const_iterator iter = headers_.begin();
    for (; iter != headers_.end(); ++iter) {
        ss << iter->first << "=" << iter->second << " ";
    }
    ss << std::endl;
    ss << "body: " << body_;
    return ss.str();
}

END_KMONITOR_NAMESPACE(kmonitor);
