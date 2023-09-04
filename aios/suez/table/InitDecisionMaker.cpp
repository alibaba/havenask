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
#include "suez/table/InitDecisionMaker.h"

#include "autil/Log.h"

using namespace std;

namespace suez {
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, InitDecisionMaker);

OperationType InitDecisionMaker::makeDecision(const CurrentPartitionMeta &current,
                                              const TargetPartitionMeta &target,
                                              const Context &ctx) const {
    auto tableStatus = current.getTableStatus();
    if (tableStatus == TS_UNKNOWN) {
        return OP_INIT;
    } else if (tableStatus == TS_INITIALIZING) {
        return OP_HOLD;
    } else {
        return OP_NONE;
    }
}

} // namespace suez
