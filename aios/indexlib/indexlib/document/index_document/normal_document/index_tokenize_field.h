#ifndef __INDEXLIB_INDEX_TOKENIZE_FIELD_H
#define __INDEXLIB_INDEX_TOKENIZE_FIELD_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/document/index_document/normal_document/section.h"
#include <autil/mem_pool/pool_allocator.h>

IE_NAMESPACE_BEGIN(document);

#pragma pack(push, 2)

class IndexTokenizeField : public Field
{
public:
    const static size_t MAX_SECTION_PER_FIELD = 256;
    
public:
    IndexTokenizeField(autil::mem_pool::Pool* pool = NULL);
    ~IndexTokenizeField();
public:
    typedef autil::mem_pool::pool_allocator<Section*> Alloc;
    typedef std::vector<Section*, Alloc> SectionVector;
    typedef SectionVector::const_iterator Iterator;

public:
    void Reset() override;
    void serialize(autil::DataBuffer &dataBuffer) const override; 
    void deserialize(autil::DataBuffer &dataBuffer) override;
    bool operator==(const Field& field) const override;
    bool operator!=(const Field& field) const override { return !(*this == field); }

public:
    //used for reusing section object
    Section* CreateSection(uint32_t tokenCount = Section::DEFAULT_TOKEN_NUM);

    void AddSection(Section* section);
    Section* GetSection(sectionid_t sectionId) const;

    size_t GetSectionCount() const { return mSectionUsed; }
    void SetSectionCount(sectionid_t sectionCount) { mSectionUsed = sectionCount; }

    Iterator Begin() const { return mSections.begin(); }
    Iterator End() const { return mSections.end(); }

private:
    autil::mem_pool::Pool mSelfPool;
    SectionVector mSections;
    sectionid_t mSectionUsed;
    
private:
    IE_LOG_DECLARE();
};

#pragma pack(pop)

DEFINE_SHARED_PTR(IndexTokenizeField);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_INDEX_TOKENIZE_FIELD_H
