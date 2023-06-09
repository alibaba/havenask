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

#include "iquan/jni/IquanDqlResponse.h"

namespace isearch {
namespace sql {

class GraphOptimizer {
public:
    GraphOptimizer() {}

    virtual ~GraphOptimizer() {}

public:
    virtual bool optimize(iquan::SqlPlan &sqlPlan) = 0;
    virtual std::string getName() = 0;
};

typedef std::shared_ptr<GraphOptimizer> GraphOptimizerPtr;

} // namespace sql
} // namespace isearch
