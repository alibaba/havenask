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

#include <utility>
#include <vector>

#include "ha3/search/ExecutorVisitor.h"

namespace isearch {
namespace search {

struct ExecutorOutput {
    bool isTermExecutor {false};
    std::vector<const QueryExecutor *> matchedQueryExecutor;
    size_t leafId {0};
};

class ExecutorMatched : public ExecutorVisitor {
public:
    const ExecutorOutput &getMatchedExecutor();
    void reset();

public:
    void visitAndExecutor(const AndQueryExecutor *executor) override;
    void visitOrExecutor(const OrQueryExecutor *executor) override;
    void visitOrV2Executor(const OrQueryMatchRowInfoExecutor *executor) override;
    void visitTermExecutor(const TermQueryExecutor *executor) override;
    void visitAndNotExecutor(const AndNotQueryExecutor *executor) override;
    void visitBitmapAndExecutor(const BitmapAndQueryExecutor *executor) override;

private:
    ExecutorOutput _matchedInfo;
};

} // namespace search
} // namespace isearch
