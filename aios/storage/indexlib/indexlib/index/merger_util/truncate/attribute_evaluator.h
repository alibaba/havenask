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
#ifndef __INDEXLIB_ATTRIBUTE_EVALUATOR_H
#define __INDEXLIB_ATTRIBUTE_EVALUATOR_H

#include <memory>

#include "autil/LongHashValue.h"
#include "indexlib/common_define.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/merger_util/truncate/evaluator.h"
#include "indexlib/index/merger_util/truncate/reference_typed.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

template <typename T>
class AttributeEvaluator : public Evaluator
{
public:
    AttributeEvaluator(AttributeSegmentReaderPtr attrReader, Reference* refer)
    {
        mRefer = dynamic_cast<index::legacy::ReferenceTyped<T>*>(refer);
        assert(mRefer);
        mAttrReader = attrReader;
        mReadCtx = mAttrReader->CreateReadContextPtr(&mPool);
    }
    AttributeEvaluator(std::shared_ptr<indexlibv2::index::AttributeDiskIndexer> attrDiskIndexer, Reference* refer)
    {
        mRefer = dynamic_cast<index::legacy::ReferenceTyped<T>*>(refer);
        assert(mRefer);
        mAttrDiskIndexer = attrDiskIndexer;
        mDiskReadCtx = mAttrDiskIndexer->CreateReadContextPtr(&mPool);
    }
    ~AttributeEvaluator()
    {
        mReadCtx.reset();
        mDiskReadCtx.reset();
        mPool.release();
    }

public:
    void Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;
    double GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter, DocInfo* docInfo) override;

private:
    ReferenceTyped<T>* mRefer;
    AttributeSegmentReaderPtr mAttrReader;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer> mAttrDiskIndexer;
    AttributeSegmentReader::ReadContextBasePtr mReadCtx;
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase> mDiskReadCtx;
    autil::mem_pool::UnsafePool mPool;

private:
    IE_LOG_DECLARE();
};

template <typename T>
inline void AttributeEvaluator<T>::Evaluate(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                            DocInfo* docInfo)
{
    T attrValue {};
    bool isNull = false;
    if (mAttrReader) {
        mAttrReader->Read(docId, mReadCtx, (uint8_t*)&attrValue, sizeof(T), isNull);
    } else {
        uint32_t dataLen = 0;
        mAttrDiskIndexer->Read(docId, mDiskReadCtx, (uint8_t*)&attrValue, sizeof(T), dataLen, isNull);
    }
    mRefer->Set(attrValue, isNull, docInfo);
}

template <typename T>
inline double AttributeEvaluator<T>::GetValue(docid_t docId, const std::shared_ptr<PostingIterator>& postingIter,
                                              DocInfo* docInfo)
{
    T attrValue {};
    bool isNull = false;
    mAttrReader->Read(docId, mReadCtx, (uint8_t*)&attrValue, sizeof(T), isNull);
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

} // namespace indexlib::index::legacy

#endif //__INDEXLIB_ATTRIBUTE_EVALUATOR_H
