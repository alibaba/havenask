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
#ifndef ISEARCH_MULTI_CALL_GIGGRPCCLOSURE_H
#define ISEARCH_MULTI_CALL_GIGGRPCCLOSURE_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/rpc/GigClosure.h"

namespace multi_call {

struct GrpcCallData;

class GigGrpcClosure : public GigClosure
{
public:
    GigGrpcClosure(GrpcCallData *callData);
    ~GigGrpcClosure();

private:
    GigGrpcClosure(const GigGrpcClosure &);
    GigGrpcClosure &operator=(const GigGrpcClosure &);

public:
    int64_t getStartTime() const override;
    void Run() override;
    ProtocolType getProtocolType() override;

private:
    // virtual for ut
    virtual void writeResult();

private:
    GrpcCallData *_callData;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigGrpcClosure);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGGRPCCLOSURE_H
