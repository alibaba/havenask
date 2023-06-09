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
#include "ha3/search/ExecutorMatched.h"

#include <iosfwd>

#include "ha3/search/AndQueryExecutor.h"
#include "ha3/search/OrQueryMatchRowInfoExecutor.h"
#include "ha3/search/TermQueryExecutor.h"

namespace isearch {
namespace search {
class AndNotQueryExecutor;
class BitmapAndQueryExecutor;
class OrQueryExecutor;
class QueryExecutor;
}  // namespace search
}  // namespace isearch

using namespace std;

namespace isearch {
namespace search {

const ExecutorOutput& ExecutorMatched::getMatchedExecutor()
{
    return _matchedInfo;
}

void ExecutorMatched::reset() {
    _matchedInfo = ExecutorOutput();
}

void ExecutorMatched::visitAndExecutor(const AndQueryExecutor *executor) {
    _matchedInfo.matchedQueryExecutor = {executor->getQueryExecutor(0)};
}

void ExecutorMatched::visitOrExecutor(const OrQueryExecutor *executor) {
}

void ExecutorMatched::visitOrV2Executor(const OrQueryMatchRowInfoExecutor *executor) {
    executor->getMatchedExecutor(_matchedInfo.matchedQueryExecutor);
}

void ExecutorMatched::visitTermExecutor(const TermQueryExecutor *executor) {
    _matchedInfo.isTermExecutor = true;
    _matchedInfo.leafId = executor->getLeafId();
}

void ExecutorMatched::visitAndNotExecutor(const AndNotQueryExecutor *executor) {
}

void ExecutorMatched::visitBitmapAndExecutor(const BitmapAndQueryExecutor *executor) {
}

} // namespace search
} // namespace isearch
