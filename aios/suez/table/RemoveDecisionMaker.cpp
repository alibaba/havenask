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
#include "suez/table/RemoveDecisionMaker.h"

#include "autil/Log.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RemoveDecisionMaker);

OperationType RemoveDecisionMaker::makeDecision(const CurrentPartitionMeta &current) const {
    if (current.hasDeployingIncVersion()) {
        return OP_CANCELDEPLOY;
    }
    switch (current.getTableStatus()) {
    case TS_UNKNOWN:
    case TS_UNLOADED:
        return OP_REMOVE;
    case TS_LOADING:
    case TS_FORCELOADING:
    case TS_PRELOADING:
        return OP_CANCELLOAD;
    case TS_INITIALIZING:
    case TS_UNLOADING:
    case TS_COMMITTING:
    case TS_ROLE_SWITCHING:
        return OP_HOLD;
    default:
        return OP_UNLOAD;
    }
}

} // namespace suez
