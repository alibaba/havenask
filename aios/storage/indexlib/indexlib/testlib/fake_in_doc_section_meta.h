#pragma once

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
