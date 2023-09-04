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
#include "navi/engine/RemoteSubGraphBase.h"
#include "navi/engine/RemoteInputPort.h"
#include "navi/engine/RemoteOutputPort.h"
#include "navi/util/ReadyBitMap.h"
#include "navi/util/CommonUtil.h"

namespace navi {

RemoteSubGraphBase::RemoteSubGraphBase(SubGraphDef *subGraphDef,
                                       GraphParam *param,
                                       const GraphDomainPtr &domain,
                                       const SubGraphBorderMapPtr &borderMap,
                                       Graph *graph)
    : SubGraphBase(subGraphDef, param, domain, borderMap, graph)
{
    _logger.addPrefix("remote");
}

RemoteSubGraphBase::~RemoteSubGraphBase() {
}

ErrorCode RemoteSubGraphBase::init() {
    return EC_NONE;
}

ErrorCode RemoteSubGraphBase::run() {
    return EC_NONE;
}

void RemoteSubGraphBase::terminate(ErrorCode ec) {
    NAVI_LOG(SCHEDULE2, "terminate, ec: %s", CommonUtil::getErrorString(ec));
}

}
