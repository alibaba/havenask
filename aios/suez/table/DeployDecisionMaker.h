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

#include "suez/table/OperationType.h"
#include "suez/table/StageDecisionMaker.h"
#include "suez/table/StateMachine.h"

namespace suez {

enum DeployEvent {
    ET_DEPLOY_NONE,
    ET_NEW_INC_VERSION,
    ET_NEW_CONFIG,
    ET_INDEX_ROOT_CHANGED,
    ET_WAIT_RESOURCE,
};

class DeployDecisionMaker final : public StageDecisionMaker {
private:
    using Context = StageDecisionMaker::Context;
    using EventType = DeployEvent;

public:
    DeployDecisionMaker();

public:
    OperationType makeDecision(const CurrentPartitionMeta &current,
                               const TargetPartitionMeta &target,
                               const Context &ctx) const override;

    OperationType makeDecision(const PartitionMeta &current,
                               const PartitionMeta &target,
                               bool deployWhenConfigChanged,
                               bool deployWhenIndexRootChanged,
                               bool isFinal) const;

private:
    void initStateMachine();
    EventType computeEvent(const PartitionMeta &current,
                           const PartitionMeta &target,
                           bool deployWhenConfigChanged,
                           bool deployWhenIndexRootChanged) const;
    OperationType rewriteFinal(const PartitionMeta &current, const PartitionMeta &target, OperationType type) const;

private:
    StateMachine<DeployStatus, EventType, OperationType> _stateMachine;
};

} // namespace suez
