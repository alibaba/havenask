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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlibv2::table {

class IndexReclaimerParam : public autil::legacy::Jsonizable
{
public:
    const static std::string AND_RECLAIM_OPERATOR;
    struct ReclaimOprand : public autil::legacy::Jsonizable {
        std::string indexName;
        std::string term;
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("reclaim_index", indexName, indexName);
            json.Jsonize("reclaim_term", term, term);
        }
    };

public:
    IndexReclaimerParam() = default;
    virtual ~IndexReclaimerParam() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void SetReclaimIndex(const std::string& reclaimIndex) { _reclaimIndex = reclaimIndex; }
    const std::string& GetReclaimIndex() const { return _reclaimIndex; }
    void SetReclaimTerms(const std::vector<std::string>& reclaimTerms) { _reclaimTerms = reclaimTerms; }
    const std::vector<std::string>& GetReclaimTerms() const { return _reclaimTerms; }
    const std::string& GetReclaimOperator() const { return _reclaimOperator; }
    const std::vector<ReclaimOprand>& GetReclaimOprands() const { return _reclaimOprands; }

public:
    void TEST_SetReclaimOprands(const std::vector<ReclaimOprand>& oprands)
    {
        _reclaimOprands = oprands;
        _reclaimOperator = AND_RECLAIM_OPERATOR;
    }

private:
    std::string _reclaimIndex;
    std::vector<std::string> _reclaimTerms;
    std::string _reclaimOperator;
    std::vector<ReclaimOprand> _reclaimOprands;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
