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

#include "indexlib/index/common/field_format/section_attribute/SectionAttributeFormatter.h"
#include "indexlib/index/inverted_index/Types.h"
namespace indexlib::index {

class MultiSectionMeta
{
public:
    MultiSectionMeta() : _fieldIds(nullptr), _sectionLens(nullptr), _sectionWeights(nullptr), _sectionCount(0) {}

    ~MultiSectionMeta() = default;

public:
    void Init(const uint8_t* dataBuf, bool hasFieldId, bool hasSectionWeight)
    {
        _sectionCount = SectionAttributeFormatter::UnpackBuffer(dataBuf, hasFieldId, hasSectionWeight, _sectionLens,
                                                                _fieldIds, _sectionWeights);
    }

    uint32_t GetSectionCount() const { return _sectionCount; }

    section_len_t GetSectionLen(uint32_t secId) const
    {
        assert(_sectionLens);
        return _sectionLens[secId];
    }

    section_fid_t GetFieldId(uint32_t secId) const
    {
        if (_fieldIds) {
            return _fieldIds[secId];
        }
        // Do not have fieldid, all section are considered to first field in package index
        return 0;
    }

    section_weight_t GetSectionWeight(uint32_t secId) const
    {
        if (_sectionWeights) {
            return _sectionWeights[secId];
        }
        return 0;
    }

private:
    section_fid_t* _fieldIds;
    section_len_t* _sectionLens;
    section_weight_t* _sectionWeights;
    uint32_t _sectionCount;
};

} // namespace indexlib::index
