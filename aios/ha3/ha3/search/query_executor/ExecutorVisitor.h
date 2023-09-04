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

#include "ha3/search/AndNotQueryExecutor.h"
#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/BitmapAndQueryExecutor.h"
#include "ha3/search/OrQueryExecutor.h"
#include "ha3/search/OrQueryMatchRowInfoExecutor.h"
#include "ha3/search/TermQueryExecutor.h"

namespace isearch {
namespace search {

class ExecutorVisitor {
public:
    virtual ~ExecutorVisitor() = default;

public:
    virtual void visitAndExecutor(const AndQueryExecutor *executor) = 0;
    virtual void visitOrExecutor(const OrQueryExecutor *executor) = 0;
    virtual void visitOrV2Executor(const OrQueryMatchRowInfoExecutor *executor) = 0;
    virtual void visitTermExecutor(const TermQueryExecutor *executor) = 0;
    virtual void visitAndNotExecutor(const AndNotQueryExecutor *executor) = 0;
    virtual void visitBitmapAndExecutor(const BitmapAndQueryExecutor *executor) = 0;
};

} // namespace search
} // namespace isearch
