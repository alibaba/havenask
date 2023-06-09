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
#include "ha3/search/OrQueryMatchRowInfoExecutor.h"

#include <algorithm>
#include <memory>

#include "ha3/search/ExecutorVisitor.h"
#include "ha3/search/QueryExecutorHeap.h"

namespace isearch {
namespace search {

OrQueryMatchRowInfoExecutor::OrQueryMatchRowInfoExecutor() {
}

OrQueryMatchRowInfoExecutor::~OrQueryMatchRowInfoExecutor() {
}

void OrQueryMatchRowInfoExecutor::accept(ExecutorVisitor *visitor) const {
    visitor->visitOrV2Executor(this);
}

void OrQueryMatchRowInfoExecutor::getMatchedExecutor(std::vector<const QueryExecutor*>& matchs) const {
    matchs.clear();
    auto collectFn = [&matchs, this](uint32_t entryId) -> void {
        matchs.push_back(getQueryExecutor(entryId));
    };
    collectMinEntries(1, _count, _qeMinHeap.data(), collectFn);
}

} // namespace search
} // namespace isearch
