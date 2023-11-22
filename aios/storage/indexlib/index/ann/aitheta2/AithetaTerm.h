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

#include "aios/storage/indexlib/index/ann/aitheta2/proto/AithetaQuery.pb.h"
#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfoBase.h"
#include "indexlib/index/common/Term.h"

namespace indexlibv2::index::ann {

class AithetaTerm : public indexlib::index::Term
{
public:
    AithetaTerm() = default;
    ~AithetaTerm() = default;

public:
    std::string GetTermName() const override { return AITHETA_TERM_NAME; }
    bool operator==(const indexlib::index::Term& term) const override { return this != &term; }
    std::string ToString() const override
    {
        std::stringstream ss;
        ss << AITHETA_TERM_NAME << ":" << indexlib::index::Term::ToString();
        if (nullptr != _searchInfo) {
            ss << ", AuxiliarySearchInfo:" << _searchInfo->ToString();
        }
        return ss.str();
    }

public:
    void SetAithetaSearchInfo(std::shared_ptr<AithetaAuxSearchInfoBase> info) { _searchInfo = info; }
    const std::shared_ptr<AithetaAuxSearchInfoBase>& GetAithetaSearchInfo() const { return _searchInfo; }

    std::shared_ptr<AithetaQueries> GetAithetaQueries() const { return _queries; }
    void SetAithetaQueries(std::shared_ptr<AithetaQueries> queries) { _queries = queries; }

public:
    static constexpr const char* AITHETA_TERM_NAME = "AithetaTerm";

private:
    std::shared_ptr<AithetaAuxSearchInfoBase> _searchInfo;
    std::shared_ptr<AithetaQueries> _queries;
};

} // namespace indexlibv2::index::ann
