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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/cava/SummaryProvider.h"
#include "ha3/common/Term.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "indexlib/indexlib.h"

namespace isearch {
namespace common {
class Ha3MatchDocAllocator;
class Request;
}  // namespace common
namespace search {
class SearchCommonResource;
}  // namespace search
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
class AttributeExpressionCreator;
class SuezCavaAllocator;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace summary {

struct SummaryQueryInfo {
    SummaryQueryInfo() {}
    SummaryQueryInfo(const std::string &queryString,
                     const std::vector<common::Term> &terms)
        : queryString(queryString)
        , terms(terms)
    {}

    std::string queryString;
    std::vector<common::Term> terms;
};

class SummaryExtractorProvider
{
public:
    SummaryExtractorProvider(const SummaryQueryInfo *queryInfo,
                             const config::FieldSummaryConfigVec *fieldSummaryConfigVec,
                             const common::Request *request,
                             suez::turing::AttributeExpressionCreator *attrExprCreator,
                             config::HitSummarySchema *hitSummarySchema,
                             search::SearchCommonResource& resource);

    ~SummaryExtractorProvider();
private:
    SummaryExtractorProvider(const SummaryExtractorProvider &);
    SummaryExtractorProvider& operator=(const SummaryExtractorProvider &);
public:
    const SummaryQueryInfo *getQueryInfo() const { return _queryInfo; }
    const config::FieldSummaryConfigVec *getFieldSummaryConfig() const
    {
        return _fieldSummaryConfigVec;
    }
    const common::Request *getRequest() const { return _request;}

    config::HitSummarySchema *getHitSummarySchema() const { return _hitSummarySchema; }

    summaryfieldid_t declareSummaryField(const std::string &fieldName,
            FieldType fieldType = ft_string, bool isMultiValue = false)
    {
        return _hitSummarySchema->declareSummaryField(fieldName, fieldType, isMultiValue);
    }

    bool fillAttributeToSummary(const std::string& attrName);
    const std::map<summaryfieldid_t, suez::turing::AttributeExpression*>&
    getFilledAttributes() const
    {
        return _attributes;
    }
    common::Tracer *getRequestTracer() { return _tracer;}
    ha3::SummaryProvider *getCavaSummaryProvider() {return &_summaryProvider;}
    suez::turing::SuezCavaAllocator *getCavaAllocator() {return _cavaAllocator;}
private:
    const SummaryQueryInfo *_queryInfo;
    const config::FieldSummaryConfigVec *_fieldSummaryConfigVec;
    const common::Request *_request;
    suez::turing::AttributeExpressionCreator *_attrExprCreator;
    config::HitSummarySchema *_hitSummarySchema;
    std::map<summaryfieldid_t, suez::turing::AttributeExpression*> _attributes;
    common::Ha3MatchDocAllocator *_matchDocAllocator;
    common::Tracer* _tracer;
    ha3::SummaryProvider _summaryProvider;
    suez::turing::SuezCavaAllocator *_cavaAllocator;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SummaryExtractorProvider> SummaryExtractorProviderPtr;

} // namespace summary
} // namespace isearch

