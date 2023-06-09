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
#ifndef __INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H
#define __INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H

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

#endif //__INDEXLIB_FAKE_SECTION_ATTRIBUTE_READER_H
