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

#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/QueryVisitor.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace common {
class AndNotQuery;
class AndQuery;
class MultiTermQuery;
class NumberQuery;
class OrQuery;
class PhraseQuery;
class RankQuery;
class TermQuery;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace search {

class TermDFVisitor : public common::QueryVisitor
{
public:
    TermDFVisitor(const TermDFMap &auxListTerm);
    ~TermDFVisitor();
private:
    TermDFVisitor(const TermDFVisitor &);
    TermDFVisitor& operator=(const TermDFVisitor &);
public:
    void visitTermQuery(const common::TermQuery *query) override;
    void visitPhraseQuery(const common::PhraseQuery *query) override;
    void visitAndQuery(const common::AndQuery *query) override;
    void visitOrQuery(const common::OrQuery *query) override;
    void visitAndNotQuery(const common::AndNotQuery *query) override;
    void visitRankQuery(const common::RankQuery *query) override;
    void visitNumberQuery(const common::NumberQuery *query) override;
    void visitMultiTermQuery(const common::MultiTermQuery *query) override;
public:
    df_t getDF() const {
        return _df;
    }
private:
    const TermDFMap &_auxListTerm;
    df_t _df;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TermDFVisitor> TermDFVisitorPtr;

} // namespace search
} // namespace isearch

