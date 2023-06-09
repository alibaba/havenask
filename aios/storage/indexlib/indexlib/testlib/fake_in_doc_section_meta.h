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
#ifndef __INDEXLIB_FAKE_IN_DOC_SECTION_META_H
#define __INDEXLIB_FAKE_IN_DOC_SECTION_META_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace testlib {

class FakeInDocSectionMeta : public index::InDocSectionMeta
{
public:
    FakeInDocSectionMeta();
    ~FakeInDocSectionMeta();

public:
    struct SectionInfo {
        section_len_t sectionLength;
        section_weight_t sectionWeight;
    };

    struct DocSectionMeta {
        std::map<int32_t, field_len_t> fieldLength;
        std::map<std::pair<int32_t, sectionid_t>, SectionInfo> fieldAndSecionInfo;
    };

public:
    // common interface
    section_len_t GetSectionLen(int32_t fieldPosition, sectionid_t sectId) const override;
    section_len_t GetSectionLenByFieldId(fieldid_t fieldId, sectionid_t sectId) const override
    {
        assert(false);
        return (section_len_t)0;
    }

    section_weight_t GetSectionWeight(int32_t fieldPosition, sectionid_t sectId) const override;
    section_weight_t GetSectionWeightByFieldId(fieldid_t fieldId, sectionid_t sectId) const override
    {
        assert(false);
        return (section_weight_t)0;
    }

    field_len_t GetFieldLen(int32_t fieldPosition) const override;
    field_len_t GetFieldLenByFieldId(fieldid_t fieldId) const override
    {
        assert(false);
        return (field_len_t)0;
    }

    void SetDocSectionMeta(const DocSectionMeta&);

    void GetSectionMeta(uint32_t idx, section_weight_t& sectionWeight, fieldid_t& fieldId,
                        section_len_t& sectionLength) const override
    {
    }

    uint32_t GetSectionCount() const override { return _docSectionMeta.fieldAndSecionInfo.size(); }

    uint32_t GetSectionCountInField(int32_t fieldPosition) const override
    {
        assert(false);
        return 0;
    }

private:
    DocSectionMeta _docSectionMeta;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeInDocSectionMeta);
}} // namespace indexlib::testlib

#endif //__INDEXLIB_FAKE_IN_DOC_SECTION_META_H
