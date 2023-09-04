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
#include "autil/NoCopyable.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/TypeTraits.h"
#include "indexlib/index/attribute/expression/AttributePrefetcher.h"
#include "indexlib/index/attribute/expression/TabletSessionResource.h"

namespace indexlibv2::index {

class AtomicAttributeExpressionBase
{
public:
    virtual ~AtomicAttributeExpressionBase() {}
    virtual Status InitPrefetcher(TabletSessionResource* resource) = 0;
    virtual Status Prefetch(const matchdoc::MatchDoc& matchDoc) = 0;
};

template <typename T>
class AtomicAttributeExpression : public expression::AttributeExpressionTyped<T>, public AtomicAttributeExpressionBase
{
public:
    AtomicAttributeExpression(const std::shared_ptr<AttributeConfig>& attrConfig)
        : expression::AttributeExpressionTyped<T>(
              expression::ET_ATOMIC, expression::AttrValueType2ExprValueType<T>::evt,
              expression::AttrValueType2ExprValueType<T>::isMulti, attrConfig->GetAttrName())
        , _attrConfig(attrConfig)
    {
    }
    ~AtomicAttributeExpression() {}

public:
    void evaluate(const matchdoc::MatchDoc& matchDoc) override;
    void batchEvaluate(matchdoc::MatchDoc* matchDocs, uint32_t docCount) override { assert(false); }
    Status InitPrefetcher(TabletSessionResource* resource) override;
    Status Prefetch(const matchdoc::MatchDoc& matchDoc) override;

private:
    std::shared_ptr<AttributeConfig> _attrConfig;
    AttributePrefetcher<T> _prefetcher;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////////////////////////
template <typename T>
Status AtomicAttributeExpression<T>::InitPrefetcher(TabletSessionResource* resource)
{
    return _prefetcher.Init(resource->sessionPool, _attrConfig, resource->segments);
}

template <typename T>
Status AtomicAttributeExpression<T>::Prefetch(const matchdoc::MatchDoc& matchDoc)
{
    return _prefetcher.Prefetch(matchDoc.getDocId());
}

template <typename T>
void AtomicAttributeExpression<T>::evaluate(const matchdoc::MatchDoc& matchDoc)
{
    // call prefetch before evaluate
    assert((docid_t)matchDoc.getDocId() == _prefetcher.GetCurrentDocId());
    T value = _prefetcher.GetValue();

    this->storeValue(matchDoc, value);
}

} // namespace indexlibv2::index
