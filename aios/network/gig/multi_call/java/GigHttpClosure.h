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
#ifndef ISEARCH_MULTI_CALL_GIGHTTPCLOSURE_H
#define ISEARCH_MULTI_CALL_GIGHTTPCLOSURE_H

#include "aios/network/gig/multi_call/java/GigJavaClosure.h"

namespace multi_call {

class GigHttpClosure : public GigJavaClosure
{
public:
    GigHttpClosure(JavaCallback callback, long callbackId, const GigRequestGeneratorPtr &generator);
    ~GigHttpClosure();

private:
    GigHttpClosure(const GigHttpClosure &);
    GigHttpClosure &operator=(const GigHttpClosure &);

public:
    virtual bool extractResponse(ResponsePtr response, GigResponseHeader *responseHeader,
                                 const char *&body, size_t &bodySize) override;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigHttpClosure);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGHTTPCLOSURE_H
