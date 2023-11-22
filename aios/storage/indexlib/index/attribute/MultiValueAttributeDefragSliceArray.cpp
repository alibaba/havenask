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
#include "indexlib/index/attribute/MultiValueAttributeDefragSliceArray.h"

#include "indexlib/framework/mem_reclaimer/IIndexMemoryReclaimer.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiValueAttributeDefragSliceArray);

MultiValueAttributeDefragSliceArray::MultiValueAttributeDefragSliceArray(
    const std::shared_ptr<indexlib::util::DefragSliceArray::SliceArray>& sliceArray, const std::string& attrName,
    uint64_t defragPercentThreshold, framework::IIndexMemoryReclaimer* indexMemoryReclaimer)
    : DefragSliceArray(sliceArray)
    , _offsetReader(nullptr)
    , _attributeMetrics(nullptr)
    , _attrName(attrName)
    , _defragPercentThreshold(defragPercentThreshold)
    , _indexMemoryReclaimer(indexMemoryReclaimer)
{
}

MultiValueAttributeDefragSliceArray::~MultiValueAttributeDefragSliceArray()
{
    std::lock_guard<std::recursive_mutex> guard(_dataMutex);
    if (_indexMemoryReclaimer) {
        auto iter = _uselessSliceIdxs.begin();
        for (; iter != _uselessSliceIdxs.end(); iter++) {
            _indexMemoryReclaimer->DropRetireItem(iter->first);
        }
    }
}

void MultiValueAttributeDefragSliceArray::Init(MultiValueAttributeOffsetReader* offsetReader,
                                               const MultiValueAttributeOffsetUpdatableFormatter& offsetFormatter,
                                               const MultiValueAttributeDataFormatter& dataFormatter,
                                               std::shared_ptr<AttributeMetrics> attributeMetrics)
{
    _offsetReader = offsetReader;
    auto fileReader = _offsetReader->GetFileReader();
    if (fileReader) {
        _filePath = fileReader->GetLogicalPath();
    }
    _offsetFormatter = offsetFormatter;
    _dataFormatter = dataFormatter;
    _attributeMetrics = attributeMetrics;
    InitMetrics(_attributeMetrics);
}

void MultiValueAttributeDefragSliceArray::memReclaim(void* addr)
{
    std::lock_guard<std::recursive_mutex> guard(_dataMutex);
    int64_t sliceIdx = (int64_t)addr;
    AUTIL_LOG(INFO, "mem reclaim slice [%ld] path [%s]", sliceIdx, _filePath.c_str());
    auto iter = _uselessSliceIdxs.begin();
    for (; iter != _uselessSliceIdxs.end(); iter++) {
        if (iter->second == sliceIdx) {
            break;
        }
    }
    if (iter != _uselessSliceIdxs.end()) {
        ReleaseSlice(sliceIdx);
        _uselessSliceIdxs.erase(iter);
        AUTIL_LOG(DEBUG, "release attribute [%s] slice [%ld]", _attrName.c_str(), sliceIdx);
    }

    if (_attributeMetrics) {
        _attributeMetrics->IncreasereclaimedSliceCountValue(1);
        _attributeMetrics->IncreasetotalReclaimedBytesValue(GetSliceLen());
    }
}

void MultiValueAttributeDefragSliceArray::DoFree(size_t size)
{
    std::lock_guard<std::recursive_mutex> guard(_dataMutex);
    if (_attributeMetrics) {
        _attributeMetrics->IncreasevarAttributeWastedBytesValue(size);
    }
}

void MultiValueAttributeDefragSliceArray::Defrag(int64_t sliceIdx)
{
    std::lock_guard<std::recursive_mutex> guard(_dataMutex);
    AUTIL_LOG(DEBUG, "Begin defrag attribute [%s], sliceIdx [%ld]", _attrName.c_str(), sliceIdx);

    int64_t sliceLen = GetSliceLen();
    // [Begin, End)
    uint64_t offsetBegin = _offsetFormatter.EncodeSliceArrayOffset(SliceIdxToOffset(sliceIdx));
    uint64_t offsetEnd = offsetBegin + sliceLen;
    size_t docCount = _offsetReader->GetDocCount();
    std::map<docid_t, uint64_t> moveDataInfo;

    for (docid_t docId = 0; docId < docCount; docId++) {
        auto [status, offset] = _offsetReader->GetOffset(docId);
        if (unlikely(!status.IsOK())) {
            AUTIL_LOG(ERROR, "read data offset fail in Defrag process for doc [%d]", docId);
            return; // abandon this defrag
        }
        if (offset >= offsetBegin && offset < offsetEnd) {
            moveDataInfo[docId] = offset;
        }
    }
    for (auto& [docId, offset] : moveDataInfo) {
        MoveData(docId, offset);
    }

    if (_attributeMetrics) {
        _attributeMetrics->IncreasecurReaderReclaimableBytesValue(sliceLen);
        int64_t movedSize = sliceLen - GetWastedSize(sliceIdx) - GetHeaderSize();
        _attributeMetrics->IncreasevarAttributeWastedBytesValue(movedSize);
    }

    SetWastedSize(sliceIdx, sliceLen);
    int64_t retireItemId = 0;
    if (_indexMemoryReclaimer) {
        AUTIL_LOG(INFO, "retire slice [%ld] path [%s]", sliceIdx, _filePath.c_str());
        retireItemId = _indexMemoryReclaimer->Retire((void*)sliceIdx, [this](void* addr) { this->memReclaim(addr); });
    }
    _uselessSliceIdxs.push_back(std::make_pair(retireItemId, sliceIdx));
    AUTIL_LOG(DEBUG, "End defrag attribute [%s], sliceIdx [%ld]", _attrName.c_str(), sliceIdx);
}

void MultiValueAttributeDefragSliceArray::AllocateNewSlice()
{
    std::lock_guard<std::recursive_mutex> guard(_dataMutex);
    _sliceArray->AllocateSlice();
    SliceHeader header;
    _sliceArray->Insert(&header, sizeof(header));
}

bool MultiValueAttributeDefragSliceArray::NeedDefrag(int64_t sliceIdx)
{
    if (sliceIdx == GetCurrentSliceIdx()) {
        return false;
    }

    size_t wastedSize = GetWastedSize(sliceIdx);
    size_t threshold = (size_t)(GetSliceLen() * (_defragPercentThreshold / 100.0));

    return wastedSize >= threshold;
}

void MultiValueAttributeDefragSliceArray::InitMetrics(std::shared_ptr<AttributeMetrics> attributeMetrics)
{
    if (!attributeMetrics) {
        return;
    }

    int64_t sliceNum = GetSliceNum();
    uint64_t sliceLen = GetSliceLen();
    int64_t totalWastedSize = 0;
    for (int64_t i = 0; i < sliceNum; ++i) {
        int32_t wastedSize = GetWastedSize(i);
        if (wastedSize != (int64_t)sliceLen) {
            totalWastedSize += wastedSize;
        }
    }
    attributeMetrics->IncreasevarAttributeWastedBytesValue(totalWastedSize);
}

} // namespace indexlibv2::index
