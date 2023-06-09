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

#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/inverted_index/truncate/IEvaluator.h"
#include "indexlib/index/inverted_index/truncate/ReferenceTyped.h"

namespace indexlib::index {

template <typename T>
class AttributeEvaluator : public IEvaluator
{
public:
    AttributeEvaluator(const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer>& attrDiskIndexer,
                       Reference* refer)
    {
        _refer = dynamic_cast<ReferenceTyped<T>*>(refer);
        assert(_refer);
        _attrDiskIndexer = attrDiskIndexer;
        _diskReadCtx = _attrDiskIndexer->CreateReadContextPtr(&_pool);
    }

    ~AttributeEvaluator()
    {
        _diskReadCtx.reset();
        _pool.release();
    }

public:
    void Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;
    double GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;

private:
    ReferenceTyped<T>* _refer;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer> _attrDiskIndexer;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase> _diskReadCtx;
    autil::mem_pool::UnsafePool _pool;
};

template <typename T>
inline void AttributeEvaluator<T>::Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                            DocInfo* docInfo)
{
    T attrValue {};
    bool isNull = false;
    assert(_attrDiskIndexer);
    uint32_t dataLen = 0;
    _attrDiskIndexer->Read(docId, _diskReadCtx, (uint8_t*)&attrValue, sizeof(T), dataLen, isNull);
    _refer->Set(attrValue, isNull, docInfo);
}

template <typename T>
inline double AttributeEvaluator<T>::GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                              DocInfo* docInfo)
{
    T attrValue {};
    bool isNull = false;
    uint32_t dataLen = 0;
    _attrDiskIndexer->Read(docId, _diskReadCtx, (uint8_t*)&attrValue, sizeof(T), dataLen, isNull);
    return (double)attrValue;
}

template <>
inline double AttributeEvaluator<autil::uint128_t>::GetValue(docid_t docId,
                                                             const std::shared_ptr<PostingIterator>& postingIter,
                                                             DocInfo* docInfo)
{
    return 0;
}

template <>
inline double AttributeEvaluator<autil::uint256_t>::GetValue(docid_t docId,
                                                             const std::shared_ptr<PostingIterator>& postingIter,
                                                             DocInfo* docInfo)
{
    return 0;
}

} // namespace indexlib::index
