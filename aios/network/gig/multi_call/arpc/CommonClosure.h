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
#ifndef ISEARCH_MULTI_CALL_COMMONCLOSURE_H
#define ISEARCH_MULTI_CALL_COMMONCLOSURE_H

#include "aios/network/gig/multi_call/rpc/GigClosure.h"

namespace multi_call {

class CommonClosure : public GigClosure
{
public:
    CommonClosure(google::protobuf::Closure *closure) : _closure(closure) {
    }
    ~CommonClosure() {
        _closure = NULL;
    }

private:
    CommonClosure(const CommonClosure &);
    CommonClosure &operator=(const CommonClosure &);

public:
    google::protobuf::Closure *getClosure() {
        return _closure;
    }
    void Run() override;
    ProtocolType getProtocolType() override {
        return MC_PROTOCOL_UNKNOWN;
    }

private:
    google::protobuf::Closure *_closure;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(CommonClosure);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_COMMONCLOSURE_H
