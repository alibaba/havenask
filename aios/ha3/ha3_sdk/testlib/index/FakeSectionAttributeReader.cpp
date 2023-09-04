#include "ha3_sdk/testlib/index/FakeSectionAttributeReader.h"

#include <utility>

#include "ha3_sdk/testlib/index/FakeInDocSectionMeta.h"

namespace indexlib {
namespace index {

InDocSectionMetaPtr FakeSectionAttributeReader::GetSection(docid_t docId) const {
    FakeInDocSectionMetaPtr ptr(new FakeInDocSectionMeta());
    std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta>::const_iterator iter
        = _sectionAttributeMap.find(docId);
    if (iter == _sectionAttributeMap.end()) {
        return ptr;
    }
    ptr->SetDocSectionMeta(iter->second);
    return ptr;
}

} // namespace index
} // namespace indexlib
