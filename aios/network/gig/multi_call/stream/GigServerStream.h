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
#ifndef ISEARCH_MULTI_CALL_GIGSERVERSTREAM_H
#define ISEARCH_MULTI_CALL_GIGSERVERSTREAM_H

#include "aios/network/gig/multi_call/stream/GigStreamBase.h"

namespace multi_call {

class GigStreamHandlerBase;

class GigServerStream : public GigStreamBase {
public:
    GigServerStream();
    virtual ~GigServerStream();

private:
    GigServerStream(const GigServerStream &);
    GigServerStream &operator=(const GigServerStream &);

public:
    bool send(PartIdTy partId, bool eof,
              google::protobuf::Message *message) override;
    void sendCancel(PartIdTy partId,
                    google::protobuf::Message *message) override;
public:
    std::shared_ptr<QueryInfo> getQueryInfo() const override;
    std::string getPeer() const override;
public:
    void setHandler(const std::shared_ptr<GigStreamHandlerBase> &handler);

private:
    std::shared_ptr<GigStreamHandlerBase> _handler;
};

MULTI_CALL_TYPEDEF_PTR(GigServerStream);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSERVERSTREAM_H
