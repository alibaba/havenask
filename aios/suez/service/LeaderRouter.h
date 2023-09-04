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
#pragma once
#include <string>
#include <vector>

#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "autil/result/Result.h"
#include "suez/service/Service.pb.h"

namespace suez {

class LeaderRouter : autil::NoCopyable {
public:
    LeaderRouter();
    virtual ~LeaderRouter();

public:
    bool init(const std::string &zoneName);

    void updateSchema(const multi_call::SearchServicePtr &searchService,
                      const UpdateSchemaRequest *request,
                      UpdateSchemaResponse *response);
    void getSchemaVersion(const multi_call::SearchServicePtr &searchService,
                          const GetSchemaVersionRequest *request,
                          GetSchemaVersionResponse *response);

private:
    void fillUpdateResponse(const multi_call::ReplyPtr &reply, UpdateSchemaResponse *response);
    void fillGetSchemaVersionResponse(const multi_call::ReplyPtr &reply, GetSchemaVersionResponse *response);

private:
    std::string _zoneName;
};
typedef std::shared_ptr<LeaderRouter> LeaderRouterPtr;

} // namespace suez
