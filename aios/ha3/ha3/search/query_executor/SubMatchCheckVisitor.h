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

#include "ha3/common/QueryVisitor.h"

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
} // namespace common
} // namespace isearch

namespace isearch {
namespace search {

class SubMatchCheckVisitor : public isearch::common::QueryVisitor {
public:
    SubMatchCheckVisitor();
    ~SubMatchCheckVisitor() {}

private:
    SubMatchCheckVisitor(const SubMatchCheckVisitor &);
    SubMatchCheckVisitor &operator=(const SubMatchCheckVisitor &);

public:
    void visitTermQuery(const isearch::common::TermQuery *query) override;
    void visitPhraseQuery(const isearch::common::PhraseQuery *query) override;
    void visitAndQuery(const isearch::common::AndQuery *query) override;
    void visitOrQuery(const isearch::common::OrQuery *query) override;
    void visitAndNotQuery(const isearch::common::AndNotQuery *query) override;
    void visitRankQuery(const isearch::common::RankQuery *query) override;
    void visitNumberQuery(const isearch::common::NumberQuery *query) override;
    void visitMultiTermQuery(const isearch::common::MultiTermQuery *query) override;
    bool needSubMatch() {
        return _needSubMatch;
    }

private:
    bool _needSubMatch;
    AUTIL_LOG_DECLARE();
};

} // namespace search
} // namespace isearch
