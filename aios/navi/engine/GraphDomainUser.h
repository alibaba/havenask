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
#ifndef NAVI_GRAPHDOMAINUSER_H
#define NAVI_GRAPHDOMAINUSER_H

#include "navi/engine/GraphDomain.h"
#include <multi_call/interface/SyncClosure.h>

namespace navi {

class NaviUserResult;

class GraphDomainUser : public GraphDomain
{
public:
    GraphDomainUser(Graph *graph);
    ~GraphDomainUser();
private:
    GraphDomainUser(const GraphDomainUser &);
    GraphDomainUser &operator=(const GraphDomainUser &);
public:
    bool run() override;
    void notifySend(NaviPartId partId) override;
    void doFinish(ErrorCode ec) override;
    void setUserResult(const std::shared_ptr<NaviUserResult> &result);
    std::shared_ptr<NaviUserResult> getUserResult();
    void fillNaviResult(const std::shared_ptr<NaviUserResult> &userResult);
private:
    mutable autil::ReadWriteLock _resultLock;
    std::shared_ptr<NaviUserResult> _result;
};

NAVI_TYPEDEF_PTR(GraphDomainUser);

}

#endif //NAVI_GRAPHDOMAINUSER_H
