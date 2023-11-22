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
#include "navi/engine/ForkGraphResult.h"

namespace navi {

void ForkGraphResult::markThisLocation(
    SingleResultLocationDef *singleLocation) const
{
    singleLocation->set_type(NRT_FORK);
    auto forkLocation = singleLocation->mutable_fork_location();
    forkLocation->set_graph_id(_graphId);
    forkLocation->set_biz_name(_bizName);
    forkLocation->set_node_name(_node);
}
}

