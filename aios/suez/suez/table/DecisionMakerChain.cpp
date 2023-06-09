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
#include "suez/table/DecisionMakerChain.h"

#include "autil/Log.h"
#include "suez/table/DeployDecisionMaker.h"
#include "suez/table/InitDecisionMaker.h"
#include "suez/table/LoadDecisionMaker.h"
#include "suez/table/RemoveDecisionMaker.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DecisionMakerChain);

DecisionMakerChain::DecisionMakerChain() { initChain(); }

DecisionMakerChain::~DecisionMakerChain() {}

void DecisionMakerChain::initChain() {
    _stages.clear();
    _stages.emplace_back(std::make_unique<InitDecisionMaker>());
    _stages.emplace_back(std::make_unique<DeployDecisionMaker>());
    _stages.emplace_back(std::make_unique<LoadDecisionMaker>());
    _removeStage = std::make_unique<RemoveDecisionMaker>();
}

OperationType DecisionMakerChain::makeDecision(const CurrentPartitionMeta &current,
                                               const TargetPartitionMeta &target,
                                               const Context &ctx) {
    for (size_t i = 0; i < _stages.size(); ++i) {
        auto op = _stages[i]->makeDecision(current, target, ctx);
        if (op != OP_NONE) {
            return op;
        }
    }
    return OP_NONE;
}

OperationType DecisionMakerChain::remove(const CurrentPartitionMeta &current) {
    return _removeStage->makeDecision(current);
}

} // namespace suez
