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
#ifndef ISEARCH_MULTI_CALL_TCPREQUEST_H
#define ISEARCH_MULTI_CALL_TCPREQUEST_H

#include "aios/network/anet/defaultpacket.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Request.h"
#include "aios/network/gig/multi_call/proto/GigCommonProto.pb.h"

namespace multi_call {

class TcpRequest : public Request {
public:
    TcpRequest(const std::shared_ptr<google::protobuf::Arena> &arena =
               std::shared_ptr<google::protobuf::Arena>())
        : Request(MC_PROTOCOL_TCP, arena)
        , _metaByTcp(META_BT_NONE)
    {
    }
    ~TcpRequest();

private:
    TcpRequest(const TcpRequest &);
    TcpRequest &operator=(const TcpRequest &);

public:
    virtual bool serializeBody(std::string &body) = 0;

public:
    bool serialize() override;
    size_t size() const override { return _body.size(); }
    void setBody(const std::string &body) { _body = body; }
    const std::string &getBody() const { return _body; }
    void fillSpan() override;
    void setMetaByTcp(GigMetaByTcp type) { _metaByTcp = type; }

public:
    anet::DefaultPacket *makeTcpPacket();

private:
    std::string _body;
    GigMetaByTcp _metaByTcp;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(TcpRequest);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_TCPREQUEST_H
