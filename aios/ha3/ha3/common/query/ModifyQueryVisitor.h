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

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class RankQuery;
class TableQuery;
class TermQuery;
class DocIdsQuery;
} // namespace common
} // namespace isearch

namespace isearch {
namespace common {

class ModifyQueryVisitor {
public:
    virtual void visitTermQuery(TermQuery *query) = 0;
    virtual void visitPhraseQuery(PhraseQuery *query) = 0;
    virtual void visitAndQuery(AndQuery *query) = 0;
    virtual void visitOrQuery(OrQuery *query) = 0;
    virtual void visitAndNotQuery(AndNotQuery *query) = 0;
    virtual void visitRankQuery(RankQuery *query) = 0;
    virtual void visitNumberQuery(NumberQuery *query) = 0;
    virtual void visitMultiTermQuery(MultiTermQuery *query) = 0;
    virtual void visitTableQuery(TableQuery *query) {}
    virtual void visitDocIdsQuery(const DocIdsQuery *query) {}
    virtual ~ModifyQueryVisitor() {}

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch
