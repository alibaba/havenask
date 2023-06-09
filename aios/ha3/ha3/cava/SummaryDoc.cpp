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
#include "ha3/cava/SummaryDoc.h"

#include <cstddef>

#include "autil/CommonMacros.h"
#include "cava/runtime/CString.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

#include "ha3/common/SummaryHit.h"

class CavaCtx;

using namespace std;
using namespace suez::turing;

namespace ha3 {

cava::lang::CString* SummaryDoc::getFieldValue(CavaCtx *ctx, cava::lang::CString* fieldName) {
    if (unlikely(_summaryHit == NULL || fieldName == NULL)) {
        return NULL;
    }
    const auto fieldValue = _summaryHit->getFieldValue(fieldName->getStr());
    if (fieldValue == NULL) {
        return NULL;
    }
    return SuezCavaAllocUtil::allocCString(ctx, fieldValue);
}

bool SummaryDoc::setFieldValue(CavaCtx *ctx, cava::lang::CString* fieldName,
                               cava::lang::CString* fieldValue)
{
    if (unlikely(_summaryHit == NULL || fieldName == NULL || fieldValue == NULL)) {
        return false;
    }
    _summaryHit->setSummaryValue(fieldName->getStr(), fieldValue->data(),
                                 fieldValue->size(), false);
    return true;
}

bool SummaryDoc::exist(CavaCtx *ctx, cava::lang::CString* fieldName) {
    if (unlikely(_summaryHit == NULL || fieldName == NULL)) {
        return false;
    }
    if (NULL == _summaryHit->getFieldValue(fieldName->getStr())) {
        return false;
    }
    return true;
}

bool SummaryDoc::clear(CavaCtx *ctx, cava::lang::CString* fieldName) {
    if (unlikely(_summaryHit == NULL || fieldName == NULL)) {
        return false;
    }
    return _summaryHit->clearFieldValue(fieldName->getStr());
}

}
