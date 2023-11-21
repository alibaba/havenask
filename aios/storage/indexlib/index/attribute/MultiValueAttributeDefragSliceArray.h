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

#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Define.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/MultiValueAttributeOffsetReader.h"
#include "indexlib/index/attribute/format/MultiValueAttributeDataFormatter.h"
#include "indexlib/index/attribute/format/MultiValueAttributeOffsetUpdatableFormatter.h"
#include "indexlib/util/slice_array/DefragSliceArray.h"

namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
}
namespace indexlibv2::index {

class MultiValueAttributeOffsetReader;
class MultiValueAttributeDefragSliceArray : public indexlib::util::DefragSliceArray
{
public:
    MultiValueAttributeDefragSliceArray(const std::shared_ptr<indexlib::util::DefragSliceArray::SliceArray>& sliceArray,
                                        const std::string& attrName, uint64_t defragPercentThreshold,
                                        framework::IIndexMemoryReclaimer* indexMemoryReclaimer);
    ~MultiValueAttributeDefragSliceArray();

public:
    void Init(MultiValueAttributeOffsetReader* offsetReader,
              const MultiValueAttributeOffsetUpdatableFormatter& offsetFormatter,
              const MultiValueAttributeDataFormatter& dataFormatter,
              std::shared_ptr<AttributeMetrics> attributeMetrics);

protected:
    void memReclaim(void* addr);

private:
    void DoFree(size_t size) override;
    void Defrag(int64_t sliceIdx) override;
    bool NeedDefrag(int64_t sliceIdx) override;
    void AllocateNewSlice() override;

    inline void MoveData(docid_t docId, uint64_t offset) __ALWAYS_INLINE;

    void InitMetrics(std::shared_ptr<AttributeMetrics> attributeMetrics);

private:
    MultiValueAttributeOffsetUpdatableFormatter _offsetFormatter;
    MultiValueAttributeDataFormatter _dataFormatter;
    MultiValueAttributeOffsetReader* _offsetReader;
    std::shared_ptr<AttributeMetrics> _attributeMetrics;
    //(retireId, uselessSliceId)
    std::vector<std::pair<int64_t, int64_t>> _uselessSliceIdxs;
    std::string _attrName;
    uint64_t _defragPercentThreshold;
    framework::IIndexMemoryReclaimer* _indexMemoryReclaimer;
    mutable std::recursive_mutex _dataMutex;
    std::string _filePath;

private:
    AUTIL_LOG_DECLARE();
};

inline void MultiValueAttributeDefragSliceArray::MoveData(docid_t docId, uint64_t offset)
{
    uint64_t sliceOffset = _offsetFormatter.DecodeToSliceArrayOffset(offset);
    const void* data = Get(sliceOffset);
    bool isNull = false;
    size_t size = _dataFormatter.GetDataLength((uint8_t*)data, isNull);
    uint64_t newSliceOffset = Append(data, size);
    uint64_t newOffset = _offsetFormatter.EncodeSliceArrayOffset(newSliceOffset);
    _offsetReader->SetOffset(docId, newOffset);
}

} // namespace indexlibv2::index
