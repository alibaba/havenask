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
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/index/inverted_index/config/PackageIndexConfig.h"

namespace indexlib::index {

class InDocMultiSectionMeta : public InDocSectionMeta, public MultiSectionMeta
{
public:
    InDocMultiSectionMeta(const std::shared_ptr<indexlibv2::config::PackageIndexConfig>& indexConfig)
        : _packageIndexConfig(indexConfig)
    {
    }

    ~InDocMultiSectionMeta() = default;

public:
    void UnpackInDocBuffer(bool hasFieldId, bool hasSectionWeight);
    uint8_t* GetDataBuffer();

    section_len_t GetSectionLen(int32_t fieldPosition, sectionid_t sectId) const override;
    section_len_t GetSectionLenByFieldId(fieldid_t fieldId, sectionid_t sectId) const override;

    section_weight_t GetSectionWeight(int32_t fieldPosition, sectionid_t sectId) const override;
    section_weight_t GetSectionWeightByFieldId(fieldid_t fieldId, sectionid_t sectId) const override;

    field_len_t GetFieldLen(int32_t fieldPosition) const override;
    field_len_t GetFieldLenByFieldId(fieldid_t fieldId) const override;

    uint32_t GetSectionCountInField(int32_t fieldPosition) const override;

    uint32_t GetSectionCount() const override { return MultiSectionMeta::GetSectionCount(); }

    void GetSectionMeta(uint32_t idx, section_weight_t& sectionWeight, int32_t& fieldPosition,
                        section_len_t& sectionLength) const override;

protected:
    inline int32_t GetFieldIdxInPack(fieldid_t fieldId) const
    {
        return _packageIndexConfig->GetFieldIdxInPack(fieldId);
    }

    inline int32_t GetFieldId(int32_t fieldPosition) const { return _packageIndexConfig->GetFieldId(fieldPosition); }

    void InitCache() const;

protected:
    uint8_t _dataBuf[MAX_SECTION_BUFFER_LEN];
    std::shared_ptr<indexlibv2::config::PackageIndexConfig> _packageIndexConfig;
    mutable std::vector<int32_t> _fieldPosition2Offset;
    mutable std::vector<int32_t> _fieldPosition2SectionCount;
    mutable std::vector<int32_t> _fieldPosition2FieldLen;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
