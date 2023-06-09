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
#include "ha3/cava/Aggregator.h"

#include <iosfwd>

#include "ha3/search/Aggregator.h"

class CavaCtx;
namespace unsafe {
class AttributeExpression;
class Reference;
}  // namespace unsafe

using namespace std;

namespace ha3 {

unsafe::AttributeExpression *Aggregator::getGroupKeyExpr(CavaCtx *ctx) {
    return (unsafe::AttributeExpression *)((isearch::search::Aggregator *)this)->getGroupKeyExpr();
}

unsafe::AttributeExpression *Aggregator::getAggFunctionExpr(CavaCtx *ctx, uint index) {
    return (unsafe::AttributeExpression *)((isearch::search::Aggregator *)this)->getAggFunctionExpr(index);
}

unsafe::Reference *Aggregator::getAggFunctionRef(CavaCtx *ctx, uint index, uint id) {
    return (unsafe::Reference *)((isearch::search::Aggregator *)this)->getAggFunctionRef(index, id);
}

unsafe::Reference *Aggregator::getGroupKeyRef(CavaCtx *ctx) {
    return (unsafe::Reference *)((isearch::search::Aggregator *)this)->getGroupKeyRef();
}

uint Aggregator::getMaxSortCount(CavaCtx *ctx) {
    return ((isearch::search::Aggregator *)this)->getMaxSortCount();
}

}

