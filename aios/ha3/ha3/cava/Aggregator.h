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

#include "cava/common/Common.h"

#include "autil/Log.h" // IWYU pragma: keep

class CavaCtx;
namespace unsafe {
class AttributeExpression;
class Reference;
}  // namespace unsafe

namespace ha3 {

class Aggregator
{
public:
    unsafe::AttributeExpression *getGroupKeyExpr(CavaCtx *ctx);
    unsafe::AttributeExpression *getAggFunctionExpr(CavaCtx *ctx, uint index);
    unsafe::Reference *getAggFunctionRef(CavaCtx *ctx, uint index, uint id);
    unsafe::Reference *getGroupKeyRef(CavaCtx *ctx);
    uint getMaxSortCount(CavaCtx *ctx);
};

}

