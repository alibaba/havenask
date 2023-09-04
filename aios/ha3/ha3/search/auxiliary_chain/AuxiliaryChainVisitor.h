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
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ModifyQueryVisitor.h"
#include "ha3/search/AuxiliaryChainDefine.h"

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

class AuxiliaryChainVisitor : public common::ModifyQueryVisitor {
public:
    AuxiliaryChainVisitor(const std::string &auxChainName,
                          const TermDFMap &termDFMap,
                          SelectAuxChainType type);
    ~AuxiliaryChainVisitor();

private:
    AuxiliaryChainVisitor(const AuxiliaryChainVisitor &);
    AuxiliaryChainVisitor &operator=(const AuxiliaryChainVisitor &);

public:
    void visitTermQuery(common::TermQuery *query) override;
    void visitPhraseQuery(common::PhraseQuery *query) override;
    void visitAndQuery(common::AndQuery *query) override;
    void visitOrQuery(common::OrQuery *query) override;
    void visitAndNotQuery(common::AndNotQuery *query) override;
    void visitRankQuery(common::RankQuery *query) override;
    void visitNumberQuery(common::NumberQuery *query) override;
    void visitMultiTermQuery(common::MultiTermQuery *query) override;

private:
    std::string _auxChainName;
    const TermDFMap &_termDFMap;
    SelectAuxChainType _type;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AuxiliaryChainVisitor> AuxiliaryChainVisitorPtr;

} // namespace search
} // namespace isearch
