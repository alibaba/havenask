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

#include "indexlib/index/inverted_index/config/TruncateProfile.h"
#include "indexlib/index/inverted_index/truncate/BucketMap.h"
#include "indexlib/index/inverted_index/truncate/DocCollector.h"
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReaderCreator.h"

namespace indexlib::index {

class SortTruncateCollector : public DocCollector
{
public:
    class Comparator
    {
    public:
        Comparator(const std::shared_ptr<BucketMap>& bucketMap) { _bucketMap = bucketMap; }

    public:
        bool operator()(docid_t left, docid_t right)
        {
            return _bucketMap->GetSortValue(left) < _bucketMap->GetSortValue(right);
        }

    private:
        std::shared_ptr<BucketMap> _bucketMap;
    };

public:
    SortTruncateCollector(uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
                          const std::shared_ptr<IDocFilterProcessor>& filterProcessor,
                          const std::shared_ptr<DocDistinctor>& docDistinctor,
                          const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator,
                          const std::shared_ptr<BucketMap>& bucketMap, int32_t memOptimizeThreshold,
                          const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
                          TruncateAttributeReaderCreator* truncateAttrCreator);
    ~SortTruncateCollector();

public:
    void ReInit() override;
    void DoCollect(const std::shared_ptr<PostingIterator>& postingIt) override;
    void Truncate() override;
    void Reset() override
    {
        DocCollector::Reset();
        if (_bucketVec) {
            _bucketVecAllocator->DeallocateBucket(_bucketVec);
            _bucketVec.reset();
        }
        _docInfos.clear();
        _currentDocCountToReserve = 0;
    }
    int64_t EstimateMemoryUse(uint32_t docCount) const override;

private:
#pragma pack(push, 1)
    struct Doc {
        template <typename T>
        T& GetRef()
        {
            return *(T*)GetBuffer();
        }
        uint8_t* GetBuffer() { return (uint8_t*)&buffer; }

        template <typename T>
        const T& GetRef() const
        {
            return *(T*)GetBuffer();
        }
        const uint8_t* GetBuffer() const { return (uint8_t*)&buffer; }

        docid_t docId;
        uint64_t buffer;
        float payload;
        bool isNull = false;
    };
#pragma pack(pop)
    template <typename T>
    class DocComp
    {
    public:
        DocComp(bool desc, bool payload) : _desc(desc), _payload(payload) {}
        ~DocComp() {}

    public:
        bool operator()(const Doc& left, const Doc& right) const
        {
            // asc or desc, let the null value placed in the last position
            if (left.isNull) {
                return false;
            }
            if (right.isNull) {
                return true;
            }
            if (!_desc) {
                if (_payload) {
                    return left.payload * left.GetRef<T>() < right.payload * right.GetRef<T>();
                } else {
                    return left.GetRef<T>() < right.GetRef<T>();
                }
            } else {
                if (_payload) {
                    return left.payload * left.GetRef<T>() > right.payload * right.GetRef<T>();
                } else {
                    return left.GetRef<T>() > right.GetRef<T>();
                }
            }
        }
        bool equal(const Doc& left, const Doc& right) const
        {
            if (left.isNull || right.isNull) {
                return left.isNull && right.isNull;
            }
            if (_payload) {
                return left.payload * left.GetRef<T>() == right.payload * right.GetRef<T>();
            } else {
                return left.GetRef<T>() == right.GetRef<T>();
            }
        }

    private:
        bool _desc;
        bool _payload;
    };

private:
    static void SortBucket(DocIdVector& bucket, const std::shared_ptr<BucketMap>& bucketMap);
    void SortLastValidBucketAndTruncate(uint64_t docCountToReserve);
    uint32_t AcquireDocCountToReserveWithDistinct();
    void SortWithDocPayload(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end, uint32_t sortDim,
                            uint64_t minDocCountToReserve = std::numeric_limits<uint64_t>::max());
    template <typename T>
    void SortTemplate(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end, uint32_t sortDim,
                      bool isSortByDocPayload, std::string sortField, uint64_t minDocCountToReserve);

private:
    std::shared_ptr<BucketVector> _bucketVec;
    std::shared_ptr<BucketMap> _bucketMap;
    int32_t _memOptimizeThreshold;
    std::shared_ptr<indexlibv2::config::TruncateProfile> _truncateProfile;
    TruncateAttributeReaderCreator* _truncateAttributeCreator;
    std::vector<Doc> _docInfos;
    uint64_t _currentDocCountToReserve;
    autil::mem_pool::UnsafePool _pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
