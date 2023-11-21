#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/indexlib.h"
#include "indexlib/testlib/fake_in_doc_section_meta.h"

namespace indexlib { namespace testlib {

class FakeSectionAttributeReader : public index::SectionAttributeReader
{
public:
    FakeSectionAttributeReader() {}
    FakeSectionAttributeReader(const std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta>& sectionMap)
    {
        _sectionAttributeMap = sectionMap;
    }
    virtual ~FakeSectionAttributeReader() { _sectionAttributeMap.clear(); }

public:
    virtual std::shared_ptr<index::InDocSectionMeta> GetSection(docid_t docId) const
    {
        FakeInDocSectionMetaPtr ptr(new FakeInDocSectionMeta());
        std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta>::const_iterator iter = _sectionAttributeMap.find(docId);
        if (iter == _sectionAttributeMap.end()) {
            return ptr;
        }
        ptr->SetDocSectionMeta(iter->second);
        return ptr;
    }

private:
    std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> _sectionAttributeMap;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FakeSectionAttributeReader);
}} // namespace indexlib::testlib
