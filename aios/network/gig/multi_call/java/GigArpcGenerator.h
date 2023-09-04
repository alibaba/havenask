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
#ifndef ISEARCH_MULTI_CALL_GIGARPCGENERATOR_H
#define ISEARCH_MULTI_CALL_GIGARPCGENERATOR_H
#include <string>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/java/GigArpcRequest.h"
#include "aios/network/gig/multi_call/java/GigRequestGenerator.h"

namespace multi_call {

class GigArpcGenerator : public GigRequestGenerator
{
public:
    GigArpcGenerator(const std::string &clusterName, const std::string &bizName,
                     const std::shared_ptr<google::protobuf::Arena> &arena =
                         std::shared_ptr<google::protobuf::Arena>())
        : GigRequestGenerator(clusterName, bizName, arena) {
    }
    ~GigArpcGenerator() {
    }

private:
    GigArpcGenerator(const GigArpcGenerator &);
    GigArpcGenerator &operator=(const GigArpcGenerator &);

private:
    RequestPtr generateRequest(const std::string &bodyStr,
                               const GigRequestPlan &requestPlan) override;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigArpcGenerator);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGARPCGENERATOR_H
