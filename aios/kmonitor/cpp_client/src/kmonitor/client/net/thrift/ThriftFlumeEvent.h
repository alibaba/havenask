/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-05-11 15:26
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_NET_THRIFT_THRIFTFLUMEEVENT_H_
#define KMONITOR_CLIENT_NET_THRIFT_THRIFTFLUMEEVENT_H_

#include <string>
#include <map>
#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class TCompactProtocol;

class ThriftFlumeEvent {
 public:
  ThriftFlumeEvent() {
  }
  ~ThriftFlumeEvent() {
  }

  void SetHeader(const std::map<std::string, std::string> &val);
  void SetBody(const std::string& val);
  const std::string& GetBody() const;
  const std::map<std::string, std::string>& GetHeader() const;
  void AddHeader(const std::string &key, const std::string &value);
  void AddHeaders(const std::map<std::string, std::string> &headers);
  uint32_t Write(TCompactProtocol* oprot) const;
  std::string ToString() const;

 private:
   ThriftFlumeEvent(const ThriftFlumeEvent &);
   ThriftFlumeEvent& operator=(const ThriftFlumeEvent &);

 private:
   std::map<std::string, std::string> headers_;
   std::string body_;
};


END_KMONITOR_NAMESPACE(kmonitor);

#endif  // KMONITOR_CLIENT_NET_THRIFT_THRIFTFLUMEEVENT_H_
