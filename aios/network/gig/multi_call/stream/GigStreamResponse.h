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
#ifndef ISEARCH_MULTI_CALL_GIGSTREAMRESPONSE_H
#define ISEARCH_MULTI_CALL_GIGSTREAMRESPONSE_H

#include "aios/network/gig/multi_call/interface/Response.h"

namespace multi_call {

class GigStreamResponse : public Response
{
public:
    GigStreamResponse();
    ~GigStreamResponse();

private:
    GigStreamResponse(const GigStreamResponse &);
    GigStreamResponse &operator=(const GigStreamResponse &);

public:
    void init(void *data) override;
    size_t size() const override {
        return _totalSize;
    }
    void setTotalSize(size_t size) {
        _totalSize = size;
    }

private:
    size_t _totalSize = 0;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigStreamResponse);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGSTREAMRESPONSE_H
