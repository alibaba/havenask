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
#include "suez/table/OperationType.h"

namespace suez {
const char *opName(OperationType op) {
    switch (op) {
    case OP_INIT:
        return "toInit";
    case OP_DEPLOY:
        return "toDeploy";
    case OP_CANCELDEPLOY:
        return "toCancelDeploy";
    case OP_UPDATERT:
        return "toUpdateRt";
    case OP_LOAD:
        return "toLoad";
    case OP_UNLOAD:
        return "toUnload";
    case OP_CANCEL:
        return "toCancel";
    case OP_REMOVE:
        return "toRemove";
    case OP_FORCELOAD:
        return "toForceLoad";
    case OP_CANCELLOAD:
        return "toCancelLoad";
    case OP_RELOAD:
        return "toReload";
    case OP_PRELOAD:
        return "toPreload";
    case OP_DIST_DEPLOY:
        return "toDistDeploy";
    case OP_HOLD:
        return "toHold";
    case OP_CLEAN_DISK:
        return "toCleanDisk";
    case OP_CLEAN_INC_VERSION:
        return "toCleanIncVersion";
    case OP_FINAL_TO_TARGET:
        return "FinalTargetToTarget";
    case OP_UPDATEKEEPCOUNT:
        return "toUpdateGenerationKeepCount";
    case OP_UPDATE_CONFIG_KEEPCOUNT:
        return "toUpdateConfigKeepCount";
    case OP_NONE:
        return "None";
    case OP_INVALID:
        return "Invalid";
    case OP_COMMIT:
        return "toCommit";
    case OP_SYNC_VERSION:
        return "toSyncVersion";
    case OP_BECOME_LEADER:
        return "becomeLeader";
    case OP_NO_LONGER_LEADER:
        return "nolongerLeader";
    default:
        return "";
    }
}

const char *OperationType_Name(OperationType op) { return opName(op); }

bool isDeployOperation(OperationType op) { return op == OP_DEPLOY || op == OP_DIST_DEPLOY || op == OP_CLEAN_DISK; }

} // namespace suez
