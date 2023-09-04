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
#ifndef ANET_DIRECTSTREAMINGCONTEXT_H_
#define ANET_DIRECTSTREAMINGCONTEXT_H_

#include <stddef.h>

#include "aios/network/anet/directpacket.h"
#include "aios/network/anet/streamingcontext.h"

namespace anet {
class DirectStreamingContext : public StreamingContext {
public:
    DirectStreamingContext() {}
    virtual ~DirectStreamingContext() override {}

    DirectPacket *getDirectPacket() { return static_cast<DirectPacket *>(_packet); }
    DirectPacket *stealDirectPacket() { return static_cast<DirectPacket *>(stealPacket()); }

    bool isPayloadCompleted() const { return _payloadCompleted; }
    void setPayloadCompleted(bool payloadCompleted) { _payloadCompleted = payloadCompleted; }

    size_t getPayloadReadOffset() const { return _payloadReadOffset; }
    void setPayloadReadOffset(size_t payloadReadOffset) { _payloadReadOffset = payloadReadOffset; }

    void reset() override {
        _payloadCompleted = true;
        _payloadReadOffset = 0UL;
        StreamingContext::reset();
    }

private:
    bool _payloadCompleted = true;
    size_t _payloadReadOffset = 0UL;
};

} // namespace anet
#endif // ANET_DIRECTSTREAMINGCONTEXT_H_
