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
#include "navi/engine/GraphDomainUser.h"
#include "navi/engine/NaviUserResult.h"

namespace navi {

GraphDomainUser::GraphDomainUser(Graph *graph)
    : GraphDomain(graph, GDT_USER)
{
}

GraphDomainUser::~GraphDomainUser() {
}

void GraphDomainUser::setUserResult(const NaviUserResultPtr &result) {
    autil::ScopedWriteLock lock(_resultLock);
    _result = result;
}

NaviUserResultPtr GraphDomainUser::getUserResult() {
    autil::ScopedReadLock lock(_resultLock);
    return _result;
}

bool GraphDomainUser::run() {
    return true;
}

void GraphDomainUser::notifySend(NaviPartId partId) {
    NAVI_LOG(SCHEDULE2, "notify send, partId: %d", partId);
    auto result = getUserResult();
    if (result) {
        result->notifyData();
    }
}

void GraphDomainUser::doFinish(ErrorCode ec) {
    setUserResult(nullptr);
}

}
