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

#include "navi/engine/GraphResult.h"

namespace navi {

class ForkGraphResult : public GraphResult {
public:
    ForkGraphResult(GraphId graphId, const std::string &bizName,
                    const std::string &node)
        : _graphId(graphId)
        , _bizName(bizName)
        , _node(node)
    {
    }
private:
    void markThisLocation(SingleResultLocationDef *singleLocation) const override;
private:
    GraphId _graphId = INVALID_GRAPH_ID;
    std::string _bizName;
    std::string _node;
};

NAVI_TYPEDEF_PTR(ForkGraphResult);

}

