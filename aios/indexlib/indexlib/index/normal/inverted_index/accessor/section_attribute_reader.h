#ifndef __INDEXLIB_SECTION_ATTRIBUTE_READER_H
#define __INDEXLIB_SECTION_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/index/normal/inverted_index/accessor/in_doc_section_meta.h"

IE_NAMESPACE_BEGIN(index);

class SectionAttributeReader
{
public:
    SectionAttributeReader() {}
    virtual ~SectionAttributeReader() {}

public:
    virtual InDocSectionMetaPtr GetSection(docid_t docId) const = 0;
    virtual fieldid_t GetFieldId(int32_t fieldIdxInPack) const
    { assert(false); return INVALID_FIELDID; }
};

typedef std::tr1::shared_ptr<SectionAttributeReader> SectionAttributeReaderPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SECTION_ATTRIBUTE_READER_H
