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
#ifndef ISEARCH_MULTI_CALL_REPLY_H
#define ISEARCH_MULTI_CALL_REPLY_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/interface/Response.h"

namespace multi_call {
class ChildNodeReply;

class Reply {
public:
    Reply();
    virtual ~Reply();

private:
    Reply(const Reply &);
    Reply &operator=(const Reply &);

public:
    // virtual for ut
    virtual std::vector<ResponsePtr> getResponses(size_t &lackCount);
    virtual std::vector<ResponsePtr> getResponses(size_t &lackCount, std::vector<LackResponseInfo> &lackInfos);
    virtual std::map<std::string, std::vector<ResponsePtr> >
    getResponseMap(size_t &lackCount);
    void setChildNodeReply(const std::shared_ptr<ChildNodeReply> &reply);

public:
    static std::string getErrorMessage(MultiCallErrorCode ec);

private:
    std::shared_ptr<ChildNodeReply> _childNodeReply;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(Reply);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_REPLY_H
