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
#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/patch/AttributePatchIterator.h"
#include "indexlib/index/attribute/patch/AttributePatchReader.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlibv2::config {
class AttributeConfig;
}
namespace indexlibv2::framework {
class Segment;
}

namespace indexlibv2::index {
class AttributePatchReader;
class AttributeFieldValue;

class SingleFieldPatchIterator : public AttributePatchIterator
{
public:
    using SegmentPatchReaderInfo = std::pair<docid_t, std::shared_ptr<AttributePatchReader>>;

public:
    SingleFieldPatchIterator(const std::shared_ptr<config::AttributeConfig>& attrConfig, bool isSub);

    ~SingleFieldPatchIterator();

public:
    Status Init(const std::vector<std::shared_ptr<framework::Segment>>& segments);
    Status Next(AttributeFieldValue& value) override;
    void Reserve(AttributeFieldValue& value) override;
    size_t GetPatchLoadExpandSize() const override;

    fieldid_t GetFieldId() const { return _attrIdentifier; }

    attrid_t GetAttributeId() const;

    // void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

    size_t GetPatchItemCount() const { return _patchItemCount; }

private:
    std::shared_ptr<AttributePatchReader> CreateSegmentPatchReader(const PatchFileInfos& patchFileInfos);

    docid_t CalculateGlobalDocId() const;

    PatchFileInfos FilterLoadedPatchFileInfos(PatchFileInfos& patchFileInfos, segmentid_t lastLoadedSegment);

    docid_t GetNextDocId() const
    {
        return _cursor < _segmentPatchReaderInfos.size() ? CalculateGlobalDocId() : INVALID_DOCID;
    }

private:
    std::vector<SegmentPatchReaderInfo> _segmentPatchReaderInfos;
    std::shared_ptr<config::AttributeConfig> _attrConfig;
    size_t _cursor;
    size_t _patchLoadExpandSize;
    size_t _patchItemCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
