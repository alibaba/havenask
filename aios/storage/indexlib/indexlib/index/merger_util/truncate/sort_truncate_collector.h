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
#ifndef __INDEXLIB_SORT_TRUNCATE_COLLECTOR_H
#define __INDEXLIB_SORT_TRUNCATE_COLLECTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/truncate_profile.h"
#include "indexlib/index/merger_util/truncate/bucket_map.h"
#include "indexlib/index/merger_util/truncate/doc_collector.h"
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader_creator.h"
#include "indexlib/indexlib.h"
namespace indexlib::index::legacy {

class SortTruncateCollector : public DocCollector
{
public:
    class Comparator
    {
    public:
        Comparator(const BucketMapPtr& bucketMap) { mBucketMap = bucketMap; }

    public:
        bool operator()(docid_t left, docid_t right)
        {
            return mBucketMap->GetSortValue(left) < mBucketMap->GetSortValue(right);
        }

    private:
        BucketMapPtr mBucketMap;
    };

public:
    SortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                          const DocFilterProcessorPtr& filterProcessor, const DocDistinctorPtr& docDistinctor,
                          const BucketVectorAllocatorPtr& bucketVecAllocator, const BucketMapPtr& bucketMap,
                          int32_t memOptimizeThreshold, const config::TruncateProfilePtr& truncateProfile,
                          TruncateAttributeReaderCreator* truncateAttrCreator);
    ~SortTruncateCollector();

public:
    void ReInit() override;
    void DoCollect(const DictKeyInfo& key, const PostingIteratorPtr& postingIt) override;
    void Truncate() override;
    void Reset() override
    {
        DocCollector::Reset();
        if (mBucketVector) {
            mBucketVecAllocator->DeallocateBucket(mBucketVector);
            mBucketVector.reset();
        }
        mDocInfos.clear();
        mCurrentDocCountToReserve = 0;
    }
    int64_t EstimateMemoryUse(uint32_t docCount) const override;

private:
#pragma pack(push, 1)
    struct Doc {
    public:
        template <typename T>
        T& GetRef()
        {
            return *(T*)GetBuffer();
        }
        uint8_t* GetBuffer() { return (uint8_t*)&mBuffer; }

        template <typename T>
        const T& GetRef() const
        {
            return *(T*)GetBuffer();
        }
        const uint8_t* GetBuffer() const { return (uint8_t*)&mBuffer; }

    public:
        docid_t mDocId;
        uint64_t mBuffer;
        float mPayload;
        bool mIsNull = false;
    };
#pragma pack(pop)
    template <typename T>
    class DocComp
    {
    public:
        DocComp(bool desc, bool payload) : mDesc(desc), mPayload(payload) {}
        ~DocComp() {}

    public:
        bool operator()(const Doc& left, const Doc& right) const
        {
            // asc or desc, let the null value placed in the last position
            if (left.mIsNull) {
                return false;
            }
            if (right.mIsNull) {
                return true;
            }
            if (!mDesc) {
                if (mPayload) {
                    return left.mPayload * left.GetRef<T>() < right.mPayload * right.GetRef<T>();
                } else {
                    return left.GetRef<T>() < right.GetRef<T>();
                }
            } else {
                if (mPayload) {
                    return left.mPayload * left.GetRef<T>() > right.mPayload * right.GetRef<T>();
                } else {
                    return left.GetRef<T>() > right.GetRef<T>();
                }
            }
        }
        bool equal(const Doc& left, const Doc& right) const
        {
            if (left.mIsNull || right.mIsNull) {
                return left.mIsNull && right.mIsNull;
            }
            if (mPayload) {
                return left.mPayload * left.GetRef<T>() == right.mPayload * right.GetRef<T>();
            } else {
                return left.GetRef<T>() == right.GetRef<T>();
            }
        }

    private:
        bool mDesc;
        bool mPayload;
    };

private:
    static void SortBucket(DocIdVector& bucket, const BucketMapPtr& bucketMap);
    void SortLastValidBucketAndTruncate(uint64_t docCountToReserve);
    uint32_t AcquireDocCountToReserveWithDistinct();
    void SortWithDocPayload(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end, uint32_t sortDim,
                            uint64_t minDocCountToReserve = std::numeric_limits<uint64_t>::max());
    template <typename T>
    void SortTemplate(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end, uint32_t sortDim,
                      bool isSortByDocPayload, std::string sortField, uint64_t minDocCountToReserve);

private:
    BucketVectorPtr mBucketVector;
    BucketMapPtr mBucketMap;
    int32_t mMemOptimizeThreshold;
    config::TruncateProfilePtr mTruncateProfile;
    TruncateAttributeReaderCreator* mTruncateAttrCreator;
    std::vector<Doc> mDocInfos;
    uint64_t mCurrentDocCountToReserve;
    autil::mem_pool::UnsafePool mPool;

private:
    friend class SortTruncateCollectorTest;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortTruncateCollector);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_SORT_TRUNCATE_COLLECTOR_H
