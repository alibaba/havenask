#ifndef __INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H
#define __INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/section_attribute_reader.h"
#include "indexlib/testlib/fake_in_doc_section_meta.h"

IE_NAMESPACE_BEGIN(testlib);

class FakeSectionAttributeReader : public index::SectionAttributeReader
{
public:
    FakeSectionAttributeReader() {}
    FakeSectionAttributeReader(
            const std::map<docid_t, FakeInDocSectionMeta::DocSectionMeta> &sectionMap)
    {
        _sectionAttributeMap = sectionMap;
    }
    virtual ~FakeSectionAttributeReader() {
        _sectionAttributeMap.clear();
    }
public:
    virtual index::InDocSectionMetaPtr GetSection(docid_t docId) const
    {
        FakeInDocSectionMetaPtr ptr(new FakeInDocSectionMeta());
        std::map<docid_t,FakeInDocSectionMeta::DocSectionMeta>::const_iterator iter =
            _sectionAttributeMap.find(docId); 
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

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H
