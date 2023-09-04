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
#ifndef ISEARCH_MULTI_CALL_GIGARPCCALLBACK_H
#define ISEARCH_MULTI_CALL_GIGARPCCALLBACK_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/service/ProtocolCallBack.h"

namespace multi_call {

class GigArpcCallBack : public ArpcCallBack
{
public:
    GigArpcCallBack(const std::string &requestStr, const CallBackPtr &callBack,
                    autil::LockFreeThreadPool *callBackThreadPool);
    ~GigArpcCallBack();

private:
    GigArpcCallBack(const GigArpcCallBack &);
    GigArpcCallBack &operator=(const GigArpcCallBack &);

public:
    void callBack() override;

public:
    const std::string &getRequestStr() {
        return _requestStr;
    }
    std::string &getResponseStr() {
        return _responseStr;
    }

private:
    std::string _requestStr;
    std::string _responseStr;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigArpcCallBack);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGARPCCALLBACK_H
