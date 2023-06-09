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
#include "indexlib/index/inverted_index/section_attribute/InDocMultiSectionMeta.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, InDocMultiSectionMeta);

uint8_t* InDocMultiSectionMeta::GetDataBuffer() { return _dataBuf; }

void InDocMultiSectionMeta::UnpackInDocBuffer(bool hasFieldId, bool hasSectionWeight)
{
    Init(_dataBuf, hasFieldId, hasSectionWeight);
}

section_len_t InDocMultiSectionMeta::GetSectionLen(int32_t fieldPosition, sectionid_t sectId) const
{
    if (_fieldPosition2Offset.empty() && GetSectionCount() > 0) {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)_fieldPosition2Offset.size()) {
        uint32_t sectionCount = _fieldPosition2SectionCount[fieldPosition];
        if ((uint32_t)sectId < sectionCount) {
            return MultiSectionMeta::GetSectionLen(sectId + _fieldPosition2Offset[fieldPosition]);
        }
    }
    return 0;
}

section_len_t InDocMultiSectionMeta::GetSectionLenByFieldId(fieldid_t fieldId, sectionid_t sectId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetSectionLen(fieldPosition, sectId);
}

section_weight_t InDocMultiSectionMeta::GetSectionWeight(int32_t fieldPosition, sectionid_t sectId) const
{
    if (_fieldPosition2Offset.empty() && GetSectionCount() > 0) {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)_fieldPosition2Offset.size()) {
        uint32_t sectionCount = _fieldPosition2SectionCount[fieldPosition];
        if ((uint32_t)sectId < sectionCount) {
            return MultiSectionMeta::GetSectionWeight(sectId + _fieldPosition2Offset[fieldPosition]);
        }
    }
    return 0;
}

section_weight_t InDocMultiSectionMeta::GetSectionWeightByFieldId(fieldid_t fieldId, sectionid_t sectId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetSectionWeight(fieldPosition, sectId);
}

field_len_t InDocMultiSectionMeta::GetFieldLen(int32_t fieldPosition) const
{
    if (_fieldPosition2Offset.empty() && GetSectionCount() > 0) {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)_fieldPosition2FieldLen.size()) {
        return _fieldPosition2FieldLen[fieldPosition];
    }
    return 0;
}

field_len_t InDocMultiSectionMeta::GetFieldLenByFieldId(fieldid_t fieldId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetFieldLen(fieldPosition);
}

void InDocMultiSectionMeta::InitCache() const
{
    fieldid_t maxFieldId = 0;
    for (int32_t i = 0; i < (int32_t)GetSectionCount(); ++i) {
        fieldid_t fieldId = MultiSectionMeta::GetFieldId(i);
        if (maxFieldId < fieldId) {
            maxFieldId = fieldId;
        }
    }

    _fieldPosition2Offset.resize(maxFieldId + 1, -1);
    _fieldPosition2SectionCount.resize(maxFieldId + 1, 0);
    _fieldPosition2FieldLen.resize(maxFieldId + 1, 0);

    for (int32_t i = 0; i < (int32_t)GetSectionCount(); ++i) {
        fieldid_t fieldId = MultiSectionMeta::GetFieldId(i);
        if (-1 == _fieldPosition2Offset[(uint8_t)fieldId]) {
            _fieldPosition2Offset[(uint8_t)fieldId] = i;
        }

        section_len_t length = MultiSectionMeta::GetSectionLen(i);
        _fieldPosition2FieldLen[(uint8_t)fieldId] += length;
        _fieldPosition2SectionCount[(uint8_t)fieldId]++;
    }
}

void InDocMultiSectionMeta::GetSectionMeta(uint32_t idx, section_weight_t& sectionWeight, int32_t& fieldPosition,
                                           section_len_t& sectionLength) const
{
    if (idx < GetSectionCount()) {
        sectionWeight = MultiSectionMeta::GetSectionWeight(idx);
        fieldPosition = MultiSectionMeta::GetFieldId(idx);
        sectionLength = MultiSectionMeta::GetSectionLen(idx);
    }
}

uint32_t InDocMultiSectionMeta::GetSectionCountInField(int32_t fieldPosition) const
{
    if (_fieldPosition2Offset.empty() && GetSectionCount() > 0) {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)_fieldPosition2FieldLen.size()) {
        return _fieldPosition2SectionCount[fieldPosition];
    }
    return 0;
}
} // namespace indexlib::index
