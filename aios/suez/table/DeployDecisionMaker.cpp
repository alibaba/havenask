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
#include "suez/table/DeployDecisionMaker.h"

#include "autil/Log.h"

using namespace std;
using namespace std::placeholders;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DeployDecisionMaker);

DeployDecisionMaker::DeployDecisionMaker() : _stateMachine(OP_HOLD) { initStateMachine(); }

OperationType DeployDecisionMaker::makeDecision(const CurrentPartitionMeta &current,
                                                const TargetPartitionMeta &target,
                                                const Context &ctx) const {
    return makeDecision(
        current, target, ctx.config.allowReloadByConfig, ctx.config.allowReloadByIndexRoot, ctx.isFinal);
}

OperationType DeployDecisionMaker::makeDecision(const PartitionMeta &current,
                                                const PartitionMeta &target,
                                                bool deployWhenConfigChanged,
                                                bool deployWhenIndexRootChanged,
                                                bool isFinal) const {
    EventType event = computeEvent(current, target, deployWhenConfigChanged, deployWhenIndexRootChanged);
    DeployStatus state = current.getDeployStatus(target.getIncVersion());
    auto op = _stateMachine.transition(state, event);
    if (isFinal) {
        op = rewriteFinal(current, target, op);
    }
    return op;
}

void DeployDecisionMaker::initStateMachine() {
    auto hold = OP_HOLD;
    auto none = OP_NONE;
    auto lackDiskQuota = OP_CLEAN_DISK;

    // (DS_DEPLOYING, *) -> OP_HOLD, just wait deploy done or cancel by other
    _stateMachine.add(DS_DEPLOYING, ET_DEPLOY_NONE, hold);
    _stateMachine.add(DS_DEPLOYING, ET_NEW_INC_VERSION, hold);
    _stateMachine.add(DS_DEPLOYING, ET_WAIT_RESOURCE, hold);
    _stateMachine.add(DS_DEPLOYING, ET_NEW_CONFIG, hold);
    _stateMachine.add(DS_DEPLOYING, ET_INDEX_ROOT_CHANGED, hold);

    _stateMachine.add(DS_DEPLOYDONE, ET_DEPLOY_NONE, none);
    _stateMachine.add(DS_DEPLOYDONE, ET_NEW_INC_VERSION, none); // maybe inc rollback
    _stateMachine.add(DS_DEPLOYDONE, ET_WAIT_RESOURCE, none);   // maybe inc rollback
    _stateMachine.add(DS_DEPLOYDONE, ET_NEW_CONFIG, OP_DIST_DEPLOY);
    _stateMachine.add(DS_DEPLOYDONE, ET_INDEX_ROOT_CHANGED, OP_DIST_DEPLOY);

    _stateMachine.add(DS_DISKQUOTA, ET_DEPLOY_NONE, lackDiskQuota);     // maybe invalid state ?
    _stateMachine.add(DS_DISKQUOTA, ET_NEW_INC_VERSION, lackDiskQuota); // maybe inc rollback
    _stateMachine.add(DS_DISKQUOTA, ET_NEW_CONFIG, lackDiskQuota);
    _stateMachine.add(DS_DISKQUOTA, ET_INDEX_ROOT_CHANGED, lackDiskQuota);
    _stateMachine.add(DS_DISKQUOTA, ET_WAIT_RESOURCE, lackDiskQuota);

#define ADD_OTHER_STATE(event, action)                                                                                 \
    _stateMachine.add(DS_UNKNOWN, event, action);                                                                      \
    _stateMachine.add(DS_CANCELLED, event, action);                                                                    \
    _stateMachine.add(DS_FAILED, event, action)

    ADD_OTHER_STATE(ET_DEPLOY_NONE, OP_DEPLOY);
    ADD_OTHER_STATE(ET_NEW_INC_VERSION, OP_DEPLOY);
    ADD_OTHER_STATE(ET_NEW_CONFIG, OP_DIST_DEPLOY);
    ADD_OTHER_STATE(ET_INDEX_ROOT_CHANGED, OP_DIST_DEPLOY);
    ADD_OTHER_STATE(ET_WAIT_RESOURCE, OP_CANCELDEPLOY);

#undef ADD_OTHER_STATE
}

DeployDecisionMaker::EventType DeployDecisionMaker::computeEvent(const PartitionMeta &current,
                                                                 const PartitionMeta &target,
                                                                 bool deployWhenConfigChanged,
                                                                 bool deployWhenIndexRootChanged) const {
    bool newConfig = deployWhenConfigChanged && !current.getConfigPath().empty() &&
                     current.getConfigPath() != target.getConfigPath();
    bool indexRootChanged = deployWhenIndexRootChanged && !current.getIndexRoot().empty() &&
                            current.getIndexRoot() != target.getIndexRoot();
    bool newIncVersion = current.getLatestDeployVersion() != target.getIncVersion();
    auto event = ET_DEPLOY_NONE;
    if (newConfig) {
        event = ET_NEW_CONFIG;
    } else if (indexRootChanged) {
        event = ET_INDEX_ROOT_CHANGED;
    } else if (newIncVersion) {
        event = ET_NEW_INC_VERSION;
    }
    if (event != ET_DEPLOY_NONE && current.hasDeployingIncVersion()) {
        event = ET_WAIT_RESOURCE;
    }
    return event;
}

OperationType
DeployDecisionMaker::rewriteFinal(const PartitionMeta &current, const PartitionMeta &target, OperationType type) const {
    if (type == OP_DIST_DEPLOY) {
        AUTIL_LOG(INFO, "final target does not support dist deploy");
        return OP_HOLD;
    } else if (type == OP_CLEAN_DISK) {
        AUTIL_LOG(INFO, "clean disk may stop service, disabled in final target");
        return OP_HOLD;
    } else if (type == OP_DEPLOY) {
        bool newConfig = !current.getConfigPath().empty() && current.getConfigPath() != target.getConfigPath();
        bool newIndexRoot = !current.getIndexRoot().empty() && current.getIndexRoot() != target.getIndexRoot();
        if (newConfig || newIndexRoot) {
            AUTIL_LOG(INFO, "config path/index root changed in final target, hold");
            return OP_HOLD;
        }
    }
    return type;
}

} // namespace suez
