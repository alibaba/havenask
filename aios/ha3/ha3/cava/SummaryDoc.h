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

class CavaCtx;
namespace cava {
namespace lang {
class CString;
}  // namespace lang
}  // namespace cava
namespace isearch {
namespace common {
class SummaryHit;
}  // namespace common
}  // namespace isearch

namespace ha3 {

class SummaryDoc
{
public:
    SummaryDoc(isearch::common::SummaryHit *summaryHit) : _summaryHit(summaryHit) {
    }
    ~SummaryDoc() {
    }
private:
    SummaryDoc(const SummaryDoc &);
    SummaryDoc& operator=(const SummaryDoc &);
public:
    cava::lang::CString* getFieldValue(CavaCtx *ctx, cava::lang::CString* fieldName);
    bool setFieldValue(CavaCtx *ctx, cava::lang::CString* fieldName, cava::lang::CString* fieldValue);
    bool exist(CavaCtx *ctx, cava::lang::CString* fieldName);
    bool clear(CavaCtx *ctx, cava::lang::CString* fieldName);

public:
    isearch::common::SummaryHit* getSummaryHit() {
        return _summaryHit;
    }
private:
    isearch::common::SummaryHit *_summaryHit;
};

}

