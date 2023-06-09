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
#include "ha3/summary/SummaryExtractorProvider.h"

#include <assert.h>
#include <stdint.h>
#include <iosfwd>

#include "alog/Logger.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"

#include "ha3/common/ConfigClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/Request.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/SummaryProfileInfo.h"
#include "ha3/search/SearchCommonResource.h"
#include "autil/Log.h"

using namespace std;
using namespace suez::turing;

using namespace isearch::config;
namespace isearch {
namespace summary {
AUTIL_LOG_SETUP(ha3, SummaryExtractorProvider);

SummaryExtractorProvider::SummaryExtractorProvider(const SummaryQueryInfo *queryInfo,
        const FieldSummaryConfigVec *fieldSummaryConfigVec,
        const common::Request *request,
        AttributeExpressionCreator *attrExprCreator,
        config::HitSummarySchema *hitSummarySchema,
        search::SearchCommonResource& resource)
    : _queryInfo(queryInfo)
    , _fieldSummaryConfigVec(fieldSummaryConfigVec)
    , _request(request)
    , _attrExprCreator(attrExprCreator)
    , _hitSummarySchema(hitSummarySchema)
    , _matchDocAllocator(resource.matchDocAllocator.get())
    , _tracer(resource.tracer)
    , _summaryProvider(request->getConfigClause()->getKVPairs(), _tracer)
    , _cavaAllocator(resource.cavaAllocator)
{
}

SummaryExtractorProvider::~SummaryExtractorProvider() {
}

bool SummaryExtractorProvider::fillAttributeToSummary(const string& attrName) {
    assert(_hitSummarySchema);
    assert(_attrExprCreator);
    if (_hitSummarySchema->getSummaryFieldInfo(attrName)) {
        return true;
    }

    AttributeExpression *attrExpr =
        _attrExprCreator->createAtomicExpr(attrName);
    if (!attrExpr || !attrExpr->allocate(_matchDocAllocator)) {
        AUTIL_LOG(WARN, "create attribute expression[%s] failed.",
                attrName.c_str());
        return false;
    }
    FieldType ft = ft_unknown;
    bool isMultiValue = attrExpr->isMultiValue();
    auto vt = attrExpr->getType();

#define FIELD_TYPE_CONVERT_HELPER(vt_type)                              \
    case vt_type:                                                       \
        ft = VariableType2FieldTypeTraits<vt_type>::FIELD_TYPE;         \
        break

    switch (vt) {
        FIELD_TYPE_CONVERT_HELPER(vt_int8);
        FIELD_TYPE_CONVERT_HELPER(vt_int16);
        FIELD_TYPE_CONVERT_HELPER(vt_int32);
        FIELD_TYPE_CONVERT_HELPER(vt_int64);

        FIELD_TYPE_CONVERT_HELPER(vt_uint8);
        FIELD_TYPE_CONVERT_HELPER(vt_uint16);
        FIELD_TYPE_CONVERT_HELPER(vt_uint32);
        FIELD_TYPE_CONVERT_HELPER(vt_uint64);

        FIELD_TYPE_CONVERT_HELPER(vt_float);
        FIELD_TYPE_CONVERT_HELPER(vt_double);
        FIELD_TYPE_CONVERT_HELPER(vt_string);
        FIELD_TYPE_CONVERT_HELPER(vt_hash_128);
    default:
        AUTIL_LOG(TRACE3, "UNKNOWN Type: %d", (int32_t)vt);
        break;
    }
#undef FIELD_TYPE_CONVERT_HELPER

    summaryfieldid_t fieldId = _hitSummarySchema->declareSummaryField(attrName,
            ft, isMultiValue);
    if (INVALID_SUMMARYFIELDID != fieldId) {
        _attributes[fieldId] = attrExpr;
    } else {
        return false;
    }

    return true;
}

} // namespace summary
} // namespace isearch
