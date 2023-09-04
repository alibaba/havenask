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
#ifndef ISEARCH_MULTI_CALL_GIG_REQUEST_GENERATOR_H
#define ISEARCH_MULTI_CALL_GIG_REQUEST_GENERATOR_H

#include <map>
#include <string>

#include "aios/network/gig/multi_call/interface/RequestGenerator.h"
#include "aios/network/gig/multi_call/proto/GigCallProto.pb.h"

namespace multi_call {

/**
 * base class of TCP, HTTP, ARPC methods
 **/
class GigRequestGenerator : public RequestGenerator
{
public:
    GigRequestGenerator(const std::string &clusterName, const std::string &bizName,
                        const std::shared_ptr<google::protobuf::Arena> &arena =
                            std::shared_ptr<google::protobuf::Arena>())
        : RequestGenerator(clusterName, bizName, arena)
        , _isMulti(false) {
    }
    ~GigRequestGenerator() {
    }
    void generate(PartIdTy partCnt, PartRequestMap &requestMap) override;
    bool generatePartRequests(const char *packet, int len, const GigRequestPlan &requestPlan);
    void setMulti(bool multi) {
        _isMulti = multi;
    }
    bool isMulti() const {
        return _isMulti;
    }

protected:
    virtual RequestPtr generateRequest(const std::string &bodyStr,
                                       const GigRequestPlan &requestPlan) {
        return RequestPtr();
    };

protected:
    std::map<PartIdTy, RequestPtr> _partRequests;
    RequestPtr _request;
    bool _isMulti; // multi call
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigRequestGenerator);

} // namespace multi_call

#endif
