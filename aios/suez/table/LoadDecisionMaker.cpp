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
#include "suez/table/LoadDecisionMaker.h"

#include "autil/Log.h"

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LoadDecisionMaker);

LoadDecisionMaker::LoadDecisionMaker() : _stateMachine(OP_NONE) { initStateMachine(); }

OperationType LoadDecisionMaker::makeDecision(const CurrentPartitionMeta &current,
                                              const TargetPartitionMeta &target,
                                              const Context &ctx) const {
    auto loadType = determineLoadType(current, target, ctx.config);
    auto op = _stateMachine.transition(current.getTableStatus(), loadType);
    if (ctx.isFinal) {
        return rewriteFinalTarget(op, ctx);
    } else {
        return rewriteTarget(op, ctx);
    }
}

OperationType LoadDecisionMaker::rewriteFinalTarget(OperationType type, const Context &ctx) const {
    if (!ctx.supportPreload) {
        return OP_NONE;
    }
    switch (type) {
    case OP_LOAD:
        return OP_PRELOAD;
    case OP_FINAL_TO_TARGET:
    case OP_FORCELOAD:
    case OP_RELOAD:
        return OP_NONE;
    default:
        return type;
    }
}

OperationType LoadDecisionMaker::rewriteTarget(OperationType type, const Context &ctx) const {
    if (!ctx.config.allowForceLoad && (type == OP_FORCELOAD || type == OP_RELOAD)) {
        return OP_LOAD;
    }
    return type;
}

LoadEvent LoadDecisionMaker::determineLoadType(const PartitionMeta &currentMeta,
                                               const PartitionMeta &targetMeta,
                                               const ScheduleConfig &options) const {
    if (targetMeta.getRoleType() == RT_LEADER && currentMeta.getRoleType() == RT_FOLLOWER) {
        return LT_BECOME_LEADER;
    } else if (targetMeta.getRoleType() == RT_FOLLOWER && currentMeta.getRoleType() == RT_LEADER) {
        return LT_NO_LONGER_LEADER;
    }

    bool hasNewIncVersion = targetMeta.getIncVersion() > currentMeta.getIncVersion();
    bool configChanged =
        currentMeta.getLoadedConfigPath() != targetMeta.getConfigPath() && !currentMeta.getLoadedConfigPath().empty();
    bool indexRootChanged =
        currentMeta.getLoadedIndexRoot() != targetMeta.getIndexRoot() && !currentMeta.getLoadedIndexRoot().empty();

    if ((options.allowReloadByConfig && configChanged) || (options.allowReloadByIndexRoot && indexRootChanged)) {
        return LT_RELOAD;
    }

    if (hasNewIncVersion) {
        return LT_LOAD;
    }

    if (currentMeta.getBranchId() != targetMeta.getBranchId()) {
        AUTIL_LOG(INFO,
                  "rollback version for branchId from [%ld] to [%ld]",
                  currentMeta.getBranchId(),
                  targetMeta.getBranchId());
        return LT_RELOAD;
    }

    if (targetMeta.getIncVersion() < currentMeta.getIncVersion()) {
        AUTIL_LOG(INFO,
                  "revert incVersion for table from [%d] to [%d]",
                  currentMeta.getIncVersion(),
                  targetMeta.getIncVersion());
        return LT_RELOAD;
    }

    return LT_NONE;
}

void LoadDecisionMaker::initStateMachine() {
    // load
    _stateMachine.add(TS_UNKNOWN, LT_LOAD, OP_INIT);
    _stateMachine.add(TS_UNLOADED, LT_LOAD, OP_LOAD);
    _stateMachine.add(TS_LOADED, LT_LOAD, OP_LOAD);
    _stateMachine.add(TS_FORCE_RELOAD, LT_LOAD, OP_RELOAD);
    _stateMachine.add(TS_PRELOADING, LT_LOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_PRELOAD_FAILED, LT_LOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_PRELOAD_FORCE_RELOAD, LT_LOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_ERROR_LACK_MEM, LT_LOAD, OP_FORCELOAD);
    _stateMachine.add(TS_ERROR_CONFIG, LT_LOAD, OP_LOAD);
    _stateMachine.add(TS_ERROR_UNKNOWN, LT_LOAD, OP_FORCELOAD);
    _stateMachine.add(TS_COMMITTING, LT_LOAD, OP_HOLD);
    _stateMachine.add(TS_COMMIT_ERROR, LT_LOAD, OP_RELOAD);
    _stateMachine.add(TS_ROLE_SWITCHING, LT_LOAD, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCH_ERROR, LT_LOAD, OP_RELOAD);

    // reload
    _stateMachine.add(TS_UNKNOWN, LT_RELOAD, OP_INIT);
    _stateMachine.add(TS_UNLOADED, LT_RELOAD, OP_LOAD);
    _stateMachine.add(TS_LOADED, LT_RELOAD, OP_RELOAD);
    _stateMachine.add(TS_FORCE_RELOAD, LT_RELOAD, OP_RELOAD);
    _stateMachine.add(TS_PRELOADING, LT_RELOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_PRELOAD_FAILED, LT_RELOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_PRELOAD_FORCE_RELOAD, LT_RELOAD, OP_FINAL_TO_TARGET);
    _stateMachine.add(TS_ERROR_LACK_MEM, LT_RELOAD, OP_RELOAD);
    _stateMachine.add(TS_ERROR_CONFIG, LT_RELOAD, OP_LOAD);
    _stateMachine.add(TS_ERROR_UNKNOWN, LT_RELOAD, OP_RELOAD);
    _stateMachine.add(TS_COMMITTING, LT_RELOAD, OP_HOLD);
    _stateMachine.add(TS_COMMIT_ERROR, LT_RELOAD, OP_RELOAD);
    _stateMachine.add(TS_ROLE_SWITCHING, LT_RELOAD, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCH_ERROR, LT_RELOAD, OP_RELOAD);

    // become leader
    _stateMachine.add(TS_UNKNOWN, LT_BECOME_LEADER, OP_INIT);
    _stateMachine.add(TS_UNLOADED, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_LOADED, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_FORCE_RELOAD, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_PRELOAD_FAILED, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_PRELOAD_FORCE_RELOAD, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_ERROR_LACK_MEM, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_ERROR_CONFIG, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_ERROR_UNKNOWN, LT_BECOME_LEADER, OP_BECOME_LEADER);
    _stateMachine.add(TS_COMMIT_ERROR, LT_BECOME_LEADER, OP_BECOME_LEADER);

    _stateMachine.add(TS_LOADING, LT_BECOME_LEADER, OP_CANCELLOAD);
    // becomeLeader is implemented by reload, maybe we should introduce TS_ROLE_SWITCHING ?
    _stateMachine.add(TS_FORCELOADING, LT_BECOME_LEADER, OP_HOLD);
    _stateMachine.add(TS_PRELOADING, LT_BECOME_LEADER, OP_CANCELLOAD);
    _stateMachine.add(TS_UNLOADING, LT_BECOME_LEADER, OP_HOLD);
    _stateMachine.add(TS_COMMITTING, LT_BECOME_LEADER, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCHING, LT_BECOME_LEADER, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCH_ERROR, LT_BECOME_LEADER, OP_RELOAD);

    // no longer leader
    _stateMachine.add(TS_UNKNOWN, LT_NO_LONGER_LEADER, OP_INIT);
    _stateMachine.add(TS_UNLOADED, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_LOADED, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_FORCE_RELOAD, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_PRELOAD_FAILED, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_PRELOAD_FORCE_RELOAD, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_ERROR_LACK_MEM, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_ERROR_CONFIG, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_ERROR_UNKNOWN, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);
    _stateMachine.add(TS_COMMIT_ERROR, LT_NO_LONGER_LEADER, OP_NO_LONGER_LEADER);

    _stateMachine.add(TS_LOADING, LT_NO_LONGER_LEADER, OP_CANCELLOAD);
    // nolongerLeader is implemented by reload, maybe we should introduce TS_ROLE_SWITCHING ?
    _stateMachine.add(TS_FORCELOADING, LT_NO_LONGER_LEADER, OP_HOLD);
    _stateMachine.add(TS_PRELOADING, LT_NO_LONGER_LEADER, OP_CANCELLOAD);
    _stateMachine.add(TS_UNLOADING, LT_NO_LONGER_LEADER, OP_HOLD);
    _stateMachine.add(TS_COMMITTING, LT_NO_LONGER_LEADER, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCHING, LT_NO_LONGER_LEADER, OP_HOLD);
    _stateMachine.add(TS_ROLE_SWITCH_ERROR, LT_NO_LONGER_LEADER, OP_RELOAD);

    // NONE
    _stateMachine.add(TS_COMMIT_ERROR, LT_NONE, OP_RELOAD);
    _stateMachine.add(TS_ROLE_SWITCH_ERROR, LT_NONE, OP_RELOAD);
}

} // namespace suez
