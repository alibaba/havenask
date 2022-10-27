#include <ha3_sdk/testlib/index/FakeSectionAttributeReader.h>
#include <stdlib.h>
#include <string>
#include <ha3/common.h>
#include <ha3_sdk/testlib/index/FakePostingMaker.h>
#include <string.h>
#include <ha3/isearch.h>

IE_NAMESPACE_BEGIN(index);

InDocSectionMetaPtr FakeSectionAttributeReader::GetSection(docid_t docId) const
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

IE_NAMESPACE_END(index);


