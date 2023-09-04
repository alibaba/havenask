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
#include "indexlib/index/inverted_index/truncate/SortTruncateCollector.h"

#include "autil/TimeUtility.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/util/MathUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, SortTruncateCollector);

SortTruncateCollector::SortTruncateCollector(
    uint64_t minDocCountToReserve, uint64_t maxDocCountToReserve,
    const std::shared_ptr<IDocFilterProcessor>& filterProcessor, const std::shared_ptr<DocDistinctor>& docDistinctor,
    const std::shared_ptr<BucketVectorAllocator>& bucketVecAllocator, const std::shared_ptr<BucketMap>& bucketMap,
    int32_t memOptimizeThreshold, const std::shared_ptr<indexlibv2::config::TruncateProfile>& truncateProfile,
    TruncateAttributeReaderCreator* truncateAttrCreator)
    : DocCollector(minDocCountToReserve, maxDocCountToReserve, filterProcessor, docDistinctor, bucketVecAllocator)
    , _bucketMap(bucketMap)
    , _memOptimizeThreshold(memOptimizeThreshold)
    , _truncateProfile(truncateProfile)
    , _truncateAttributeCreator(truncateAttrCreator)
    , _currentDocCountToReserve(0)
{
}

SortTruncateCollector::~SortTruncateCollector() { _pool.release(); }

void SortTruncateCollector::ReInit()
{
    if (_truncateProfile->IsSortByDocPayload()) {
        return;
    }
    _bucketVec = _bucketVecAllocator->AllocateBucket();
    assert(_bucketVec);
    uint32_t bucketCount = _bucketMap->GetBucketCount();
    uint32_t bucketSize = _bucketMap->GetBucketSize();
    uint32_t bucketReserveSize = util::MathUtil::MultiplyByPercentage(bucketSize, _memOptimizeThreshold / 2);
    _bucketVec->resize(bucketCount);
    for (uint32_t i = 0; i < bucketCount; ++i) {
        if ((*_bucketVec)[i].capacity() > bucketReserveSize) {
            DocIdVector emptyVec;
            (*_bucketVec)[i].swap(emptyVec);
            (*_bucketVec)[i].reserve(bucketReserveSize);
        } else {
            (*_bucketVec)[i].clear();
        }
    }
}

void SortTruncateCollector::DoCollect(const std::shared_ptr<PostingIterator>& postingIt)
{
    docid_t docId = 0;
    if (_truncateProfile->IsSortByDocPayload()) {
        autil::ScopedTime2 timer;
        bool docPayloadUseFP16 = _truncateProfile->DocPayloadUseFP16();
        while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
            if (_filterProcessor != nullptr && _filterProcessor->IsFiltered(docId)) {
                continue;
            }
            Doc doc;
            doc.docId = docId;
            docpayload_t payload = postingIt->GetDocPayload();
            if (docPayloadUseFP16) {
                uint16_t* dst = reinterpret_cast<uint16_t*>(&(doc.payload));
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
                doc.payload = payload;
            }
            _docInfos.emplace_back(doc);
        }
        SortWithDocPayload(_docInfos.begin(), _docInfos.end(), 0, _minDocCountToReserve);
        AUTIL_LOG(DEBUG, "do collect docids by docpayload, size [%lu], time used [%.3f]s", _docInfos.size(),
                  timer.done_sec());
    } else {
        BucketVector& bucketVec = (*_bucketVec);

        uint32_t validBucketCursor = bucketVec.size() - 1;
        uint32_t currentDocCount = 0;
        uint32_t maxDocCountToReserve = _maxDocCountToReserve;

        while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
            if (_filterProcessor != nullptr && _filterProcessor->IsFiltered(docId)) {
                continue;
            }
            uint32_t bucketValue = _bucketMap->GetBucketValue(docId);
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
    if (_truncateProfile->IsSortByDocPayload()) {
        DocIdVector& docIdVec = (*_docIdVec);
        assert(docIdVec.size() == 0);
        if (_currentDocCountToReserve == 0) {
            return;
        }
        for (int i = 0; i < _currentDocCountToReserve; ++i) {
            docIdVec.push_back(_docInfos[i].docId);
        }
        _minValueDocId = docIdVec.back();
        std::sort(docIdVec.begin(), docIdVec.end());
    } else {
        uint32_t docCountToReserve = _minDocCountToReserve;
        if (_docDistinctor != nullptr) {
            docCountToReserve = AcquireDocCountToReserveWithDistinct();
            if (docCountToReserve <= _minDocCountToReserve) {
                docCountToReserve = _minDocCountToReserve;
            } else if (docCountToReserve >= _maxDocCountToReserve) {
                docCountToReserve = _maxDocCountToReserve;
            }
        }
        SortLastValidBucketAndTruncate(docCountToReserve);
    }
}

void SortTruncateCollector::SortBucket(DocIdVector& bucket, const std::shared_ptr<BucketMap>& bucketMap)
{
    Comparator comp(bucketMap);
    std::sort(bucket.begin(), bucket.end(), comp);
}

uint32_t SortTruncateCollector::AcquireDocCountToReserveWithDistinct()
{
    BucketVector& bucketVec = (*_bucketVec);
    uint32_t docCountToReserve = 0;
    uint32_t maxDocCountToReserve = _maxDocCountToReserve;
    uint32_t minDocCountToReserve = _minDocCountToReserve;
    for (uint32_t i = 0; i < bucketVec.size(); ++i) {
        if (docCountToReserve + bucketVec[i].size() > minDocCountToReserve) {
            SortBucket(bucketVec[i], _bucketMap);
        }
        for (uint32_t j = 0; j < bucketVec[i].size(); ++j) {
            docCountToReserve++;
            if (docCountToReserve >= maxDocCountToReserve || _docDistinctor->Distinct(bucketVec[i][j])) {
                return docCountToReserve;
            }
        }
    }
    return docCountToReserve;
}

void SortTruncateCollector::SortLastValidBucketAndTruncate(uint64_t docCountToReserve)
{
    BucketVector& bucketVec = (*_bucketVec);
    DocIdVector& docIdVec = (*_docIdVec);
    uint32_t totalCount = 0;
    for (size_t i = 0; i < bucketVec.size(); ++i) {
        bool finish = false;
        uint32_t count = bucketVec[i].size();
        if (totalCount + bucketVec[i].size() >= docCountToReserve ||
            i == bucketVec.size() - 1) // make sure last bucket ordered
        {
            SortBucket(bucketVec[i], _bucketMap);
            count = std::min(docCountToReserve - totalCount, bucketVec[i].size());
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
    _minValueDocId = docIdVec.back();
    std::sort(docIdVec.begin(), docIdVec.end());
}

int64_t SortTruncateCollector::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = DocCollector::EstimateMemoryUse(docCount);
    size += sizeof(docid_t) * docCount * 2; // _bucketVec
    return size;
}

void SortTruncateCollector::SortWithDocPayload(std::vector<Doc>::iterator begin, std::vector<Doc>::iterator end,
                                               uint32_t sortDim, uint64_t minDocCountToReserve)
{
    const config::SortParams& sortParams = _truncateProfile->sortParams;
    std::string sortField = sortParams[sortDim].GetSortField();
    bool isSortByDocPayload = indexlibv2::config::TruncateProfileConfig::IsSortParamByDocPayload(sortParams[sortDim]);
    uint64_t docCountToReserve = std::min(minDocCountToReserve, (uint64_t)(end - begin));
    if (isSortByDocPayload) {
        sortField = _truncateProfile->GetDocPayloadFactorField();
        if (sortField.empty()) {
            return SortTemplate<uint64_t>(begin, end, sortDim, true, sortField, docCountToReserve);
        }
    }
    auto attrConfig = _truncateAttributeCreator->GetAttributeConfig(sortField);
    assert(attrConfig != nullptr);
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
                                         uint32_t sortDim, bool isSortByDocPayload, std::string sortField,
                                         uint64_t minDocCountToReserve)
{
    if (minDocCountToReserve == 0) {
        return;
    }
    const config::SortParams& sortParams = _truncateProfile->sortParams;
    if (isSortByDocPayload && sortField.empty()) {
        for (std::vector<Doc>::iterator it = begin; it != end; ++it) {
            *(T*)it->GetBuffer() = 1;
        }
    } else {
        auto attrReader = _truncateAttributeCreator->Create(sortField);
        auto readCtx = attrReader->CreateReadContextPtr(&_pool);
        for (std::vector<Doc>::iterator it = begin; it != end; ++it) {
            uint32_t dataLen = 0;
            attrReader->Read(it->docId, readCtx, it->GetBuffer(), sizeof(T), dataLen, it->isNull);
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
            std::swap(*it, *(--lEq));
        }
        if (it == begin) {
            break;
        }
    }
    for (std::vector<Doc>::iterator it = right; it != end; it++) {
        if (comp.equal(*it, *left)) {
            std::swap(*it, *rEq);
            rEq++;
        }
    }
    if (rEq - lEq > 1 && rEq > right) {
        if (_docDistinctor) {
            for (auto it = begin; it != lEq; it++) {
                _docDistinctor->Distinct(it->docId);
            }
        }
        if (_docDistinctor && !_docDistinctor->IsFull() && lEq != begin) {
            _currentDocCountToReserve += lEq - begin;
            uint64_t docCount = std::min(_maxDocCountToReserve - _currentDocCountToReserve,
                                         std::max(minDocCountToReserve - (lEq - begin), _docDistinctor->GetLeftCnt()));
            SortWithDocPayload(lEq, end, sortDim, docCount);
        } else {
            if (sortDim == sortParams.size() - 1) {
                _currentDocCountToReserve += minDocCountToReserve;
            } else {
                _currentDocCountToReserve += lEq - begin;
                SortWithDocPayload(lEq, rEq, sortDim + 1, minDocCountToReserve - (lEq - begin));
            }
        }
    } else {
        if (_docDistinctor) {
            for (auto it = begin; it != begin + minDocCountToReserve; it++) {
                _docDistinctor->Distinct(it->docId);
            }
            if (!_docDistinctor->IsFull()) {
                _currentDocCountToReserve += minDocCountToReserve;
                uint64_t docCount =
                    std::min(_maxDocCountToReserve - _currentDocCountToReserve, _docDistinctor->GetLeftCnt());
                SortWithDocPayload(right, end, sortDim, docCount);
            } else {
                _currentDocCountToReserve += minDocCountToReserve;
            }
        } else {
            _currentDocCountToReserve += minDocCountToReserve;
        }
    }
}

} // namespace indexlib::index
