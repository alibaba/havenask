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
#include "indexlib/index/merger_util/truncate/sort_truncate_collector.h"

#include "autil/TimeUtility.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/util/MathUtil.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::config;
namespace indexlib::index::legacy {

IE_LOG_SETUP(index, SortTruncateCollector);

SortTruncateCollector::SortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                                             const DocFilterProcessorPtr& filterProcessor,
                                             const DocDistinctorPtr& docDistinctor,
                                             const BucketVectorAllocatorPtr& bucketVecAllocator,
                                             const BucketMapPtr& bucketMap, int32_t memOptimizeThreshold,
                                             const TruncateProfilePtr& truncateProfile,
                                             TruncateAttributeReaderCreator* truncateAttrCreator)
    : DocCollector(minDocCountToReserve, maxDocCountToReserve, filterProcessor, docDistinctor, bucketVecAllocator)
    , mBucketMap(bucketMap)
    , mMemOptimizeThreshold(memOptimizeThreshold)
    , mTruncateProfile(truncateProfile)
    , mTruncateAttrCreator(truncateAttrCreator)
    , mCurrentDocCountToReserve(0)
{
}

SortTruncateCollector::~SortTruncateCollector() { mPool.release(); }

void SortTruncateCollector::ReInit()
{
    if (mTruncateProfile->IsSortByDocPayload()) {
        return;
    }
    mBucketVector = mBucketVecAllocator->AllocateBucket();
    assert(mBucketVector);
    uint32_t bucketCount = mBucketMap->GetBucketCount();
    uint32_t bucketSize = mBucketMap->GetBucketSize();
    uint32_t bucketReserveSize = MathUtil::MultiplyByPercentage(bucketSize, mMemOptimizeThreshold / 2);
    mBucketVector->resize(bucketCount);
    for (uint32_t i = 0; i < bucketCount; ++i) {
        if ((*mBucketVector)[i].capacity() > bucketReserveSize) {
            DocIdVector emptyVec;
            (*mBucketVector)[i].swap(emptyVec);
            (*mBucketVector)[i].reserve(bucketReserveSize);
        } else {
            (*mBucketVector)[i].clear();
        }
    }
}

void SortTruncateCollector::DoCollect(const DictKeyInfo& key, const PostingIteratorPtr& postingIt)
{
    DocFilterProcessor* filter = mFilterProcessor.get();

    docid_t docId = 0;
    if (mTruncateProfile->IsSortByDocPayload()) {
        autil::ScopedTime2 timer;
        bool docPayloadUseFP16 = mTruncateProfile->DocPayloadUseFP16();
        while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
            if (filter && filter->IsFiltered(docId)) {
                continue;
            }
            Doc doc;
            doc.mDocId = docId;
            docpayload_t payload = postingIt->GetDocPayload();
            if (docPayloadUseFP16) {
                uint16_t* dst = reinterpret_cast<uint16_t*>(&(doc.mPayload));
                uint16_t* src = reinterpret_cast<uint16_t*>(&payload);
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
                {
                    dst[0] = *src;
                    dst[1] = 0;
                }
#else
                {
                    dst[0] = 0;
                    dst[1] = *src;
                }
#endif
            } else {
                doc.mPayload = payload;
            }
            mDocInfos.emplace_back(doc);
        }
        SortWithDocPayload(mDocInfos.begin(), mDocInfos.end(), 0, mMinDocCountToReserve);
        IE_LOG(DEBUG, "do collect docids by docpayload, size [%lu], time used [%.3f]s", mDocInfos.size(),
               timer.done_sec());
    } else {
        BucketMap* bucketMap = mBucketMap.get();
        BucketVector& bucketVec = (*mBucketVector);
        uint32_t validBucketCursor = bucketVec.size() - 1;
        uint32_t currentDocCount = 0;
        uint32_t maxDocCountToReserve = mMaxDocCountToReserve;

        while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
            if (filter && filter->IsFiltered(docId)) {
                continue;
            }
            uint32_t bucketValue = bucketMap->GetBucketValue(docId);
            if (bucketValue > validBucketCursor) {
                continue;
            }
            bucketVec[bucketValue].push_back(docId);
            currentDocCount++;

            uint32_t lastValidBucketSize = bucketVec[validBucketCursor].size();
            if (currentDocCount > maxDocCountToReserve + lastValidBucketSize && validBucketCursor > 0) {
                validBucketCursor--;
                currentDocCount -= lastValidBucketSize;
            }
        }
    }
}

void SortTruncateCollector::Truncate()
{
    if (mTruncateProfile->IsSortByDocPayload()) {
        DocIdVector& docIdVec = (*mDocIdVec);
        assert(docIdVec.size() == 0);
        if (mCurrentDocCountToReserve == 0) {
            return;
        }
        for (int i = 0; i < mCurrentDocCountToReserve; ++i) {
            docIdVec.push_back(mDocInfos[i].mDocId);
        }
        mMinValueDocId = docIdVec.back();
        sort(docIdVec.begin(), docIdVec.end());
    } else {
        uint32_t docCountToReserve = mMinDocCountToReserve;
        if (mDocDistinctor) {
            docCountToReserve = AcquireDocCountToReserveWithDistinct();
            if (docCountToReserve <= mMinDocCountToReserve) {
                docCountToReserve = mMinDocCountToReserve;
            } else if (docCountToReserve >= mMaxDocCountToReserve) {
                docCountToReserve = mMaxDocCountToReserve;
            }
        }
        SortLastValidBucketAndTruncate(docCountToReserve);
    }
}

void SortTruncateCollector::SortBucket(DocIdVector& bucket, const BucketMapPtr& bucketMap)
{
    Comparator comp(bucketMap);
    sort(bucket.begin(), bucket.end(), comp);
}

void SortTruncateCollector::SortWithDocPayload(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end,
                                               uint32_t sortDim, uint64_t minDocCountToReserve)
{
    const config::SortParams& sortParams = mTruncateProfile->mSortParams;
    std::string sortField = sortParams[sortDim].GetSortField();
    bool isSortByDocPayload = TruncateProfile::IsSortParamByDocPayload(sortParams[sortDim]);
    uint64_t docCountToReserve = min(minDocCountToReserve, (uint64_t)(end - begin));
    if (isSortByDocPayload) {
        sortField = mTruncateProfile->GetDocPayloadFactorField();
        if (sortField.empty()) {
            return SortTemplate<uint64_t>(begin, end, sortDim, true, sortField, docCountToReserve);
        }
    }
    const AttributeSchemaPtr& attrSchema = mTruncateAttrCreator->GetAttributeSchema();
    const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(sortField);
    assert(attrConfig);
    switch (attrConfig->GetFieldType()) {
#define MACRO(type)                                                                                                    \
    case type: {                                                                                                       \
        return SortTemplate<FieldTypeTraits<type>::AttrItemType>(begin, end, sortDim, isSortByDocPayload, sortField,   \
                                                                 docCountToReserve);                                   \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
    default:
        assert(false);
    }
}

template <typename T>
void SortTruncateCollector::SortTemplate(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end,
                                         uint32_t sortDim, bool isSortByDocPayload, string sortField,
                                         uint64_t minDocCountToReserve)
{
    if (minDocCountToReserve == 0) {
        return;
    }
    const config::SortParams& sortParams = mTruncateProfile->mSortParams;
    if (isSortByDocPayload && sortField.empty()) {
        for (std::vector<Doc>::iterator it = begin; it != end; ++it) {
            *(T*)it->GetBuffer() = 1;
        }
    } else {
        TruncateAttributeReaderPtr attrReader = mTruncateAttrCreator->Create(sortField);
        AttributeSegmentReader::ReadContextBasePtr readCtx = attrReader->CreateReadContextPtr(&mPool);
        for (std::vector<Doc>::iterator it = begin; it != end; ++it) {
            attrReader->Read(it->mDocId, readCtx, it->GetBuffer(), sizeof(T), it->mIsNull);
        }
    }
    DocComp<T> comp(sortParams[sortDim].IsDesc(), isSortByDocPayload);
    nth_element(begin, begin + minDocCountToReserve - 1, end, comp);

    std::vector<Doc>::iterator left = begin + minDocCountToReserve - 1;
    std::vector<Doc>::iterator right = begin + minDocCountToReserve;
    auto rEq = right;
    auto lEq = right;
    for (std::vector<Doc>::iterator it = left;; it--) {
        if (comp.equal(*it, *left)) {
            swap(*it, *(--lEq));
        }
        if (it == begin) {
            break;
        }
    }
    for (std::vector<Doc>::iterator it = right; it != end; it++) {
        if (comp.equal(*it, *left)) {
            swap(*it, *rEq);
            rEq++;
        }
    }
    if (rEq - lEq > 1 && rEq > right) {
        if (mDocDistinctor) {
            for (auto it = begin; it != lEq; it++) {
                mDocDistinctor->Distinct(it->mDocId);
            }
        }
        if (mDocDistinctor && !mDocDistinctor->IsFull() && lEq != begin) {
            mCurrentDocCountToReserve += lEq - begin;
            uint64_t docCount = min(mMaxDocCountToReserve - mCurrentDocCountToReserve,
                                    max(minDocCountToReserve - (lEq - begin), mDocDistinctor->GetLeftCnt()));
            SortWithDocPayload(lEq, end, sortDim, docCount);
        } else {
            if (sortDim == sortParams.size() - 1) {
                mCurrentDocCountToReserve += minDocCountToReserve;
            } else {
                mCurrentDocCountToReserve += lEq - begin;
                SortWithDocPayload(lEq, rEq, sortDim + 1, minDocCountToReserve - (lEq - begin));
            }
        }
    } else {
        if (mDocDistinctor) {
            for (auto it = begin; it != begin + minDocCountToReserve; it++) {
                mDocDistinctor->Distinct(it->mDocId);
            }
            if (!mDocDistinctor->IsFull()) {
                mCurrentDocCountToReserve += minDocCountToReserve;
                uint64_t docCount =
                    min(mMaxDocCountToReserve - mCurrentDocCountToReserve, mDocDistinctor->GetLeftCnt());
                SortWithDocPayload(right, end, sortDim, docCount);
            } else {
                mCurrentDocCountToReserve += minDocCountToReserve;
            }
        } else {
            mCurrentDocCountToReserve += minDocCountToReserve;
        }
    }
}

uint32_t SortTruncateCollector::AcquireDocCountToReserveWithDistinct()
{
    BucketVector& bucketVec = (*mBucketVector);
    uint32_t docCountToReserve = 0;
    DocDistinctor* docDistinct = mDocDistinctor.get();
    uint32_t maxDocCountToReserve = mMaxDocCountToReserve;
    uint32_t minDocCountToReserve = mMinDocCountToReserve;
    for (uint32_t i = 0; i < bucketVec.size(); ++i) {
        if (docCountToReserve + bucketVec[i].size() > minDocCountToReserve) {
            SortBucket(bucketVec[i], mBucketMap);
        }
        for (uint32_t j = 0; j < bucketVec[i].size(); ++j) {
            docCountToReserve++;
            if (docCountToReserve >= maxDocCountToReserve || docDistinct->Distinct(bucketVec[i][j])) {
                return docCountToReserve;
            }
        }
    }
    return docCountToReserve;
}

void SortTruncateCollector::SortLastValidBucketAndTruncate(uint64_t docCountToReserve)
{
    BucketVector& bucketVec = (*mBucketVector);
    DocIdVector& docIdVec = (*mDocIdVec);
    uint32_t totalCount = 0;
    for (size_t i = 0; i < bucketVec.size(); ++i) {
        bool finish = false;
        uint32_t count = bucketVec[i].size();
        if (totalCount + bucketVec[i].size() >= docCountToReserve ||
            i == bucketVec.size() - 1) // make sure last bucket ordered
        {
            SortBucket(bucketVec[i], mBucketMap);
            count = min(docCountToReserve - totalCount, bucketVec[i].size());
            finish = true;
        }
        if (count > 0) {
            docIdVec.insert(docIdVec.end(), bucketVec[i].begin(), bucketVec[i].begin() + count);
        }
        if (finish) {
            break;
        }
        totalCount += count;
    }
    if (docIdVec.empty()) {
        return;
    }
    // depends on last bucket is ordered.
    mMinValueDocId = docIdVec.back();
    sort(docIdVec.begin(), docIdVec.end());
}

int64_t SortTruncateCollector::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = DocCollector::EstimateMemoryUse(docCount);
    size += sizeof(docid_t) * docCount * 2; // mBucketVector
    return size;
}
} // namespace indexlib::index::legacy
