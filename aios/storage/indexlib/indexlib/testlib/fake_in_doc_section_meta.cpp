#include "indexlib/testlib/fake_in_doc_section_meta.h"

using namespace std;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeInDocSectionMeta);

FakeInDocSectionMeta::FakeInDocSectionMeta() {}

FakeInDocSectionMeta::~FakeInDocSectionMeta() {}

void FakeInDocSectionMeta::SetDocSectionMeta(const DocSectionMeta& docSecMeta) { _docSectionMeta = docSecMeta; }

section_len_t FakeInDocSectionMeta::GetSectionLen(int32_t fieldPosition, sectionid_t sectId) const
{
    std::pair<fieldid_t, sectionid_t> p = std::make_pair(fieldPosition, sectId);
    std::map<std::pair<int32_t, sectionid_t>, SectionInfo>::const_iterator iter;
    if ((iter = _docSectionMeta.fieldAndSecionInfo.find(p)) != _docSectionMeta.fieldAndSecionInfo.end()) {
        return iter->second.sectionLength;
    }
    return (section_len_t)0;
}

section_weight_t FakeInDocSectionMeta::GetSectionWeight(int32_t fieldPosition, sectionid_t sectId) const
{
    std::pair<fieldid_t, sectionid_t> p = std::make_pair(fieldPosition, sectId);
    std::map<std::pair<int32_t, sectionid_t>, SectionInfo>::const_iterator iter;
    if ((iter = _docSectionMeta.fieldAndSecionInfo.find(p)) != _docSectionMeta.fieldAndSecionInfo.end()) {
        return iter->second.sectionWeight;
    }
    return (section_weight_t)0;
}

field_len_t FakeInDocSectionMeta::GetFieldLen(fieldid_t fieldPosition) const
{
    std::map<int32_t, field_len_t>::const_iterator iter = _docSectionMeta.fieldLength.find(fieldPosition);
    if (iter != _docSectionMeta.fieldLength.end()) {
        return iter->second;
    }
    return (field_len_t)0;
}
}} // namespace indexlib::testlib
