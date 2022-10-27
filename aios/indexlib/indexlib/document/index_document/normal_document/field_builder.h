#ifndef __INDEXLIB_FIELD_BUILDER_H
#define __INDEXLIB_FIELD_BUILDER_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>
#include "indexlib/document/index_document/normal_document/normal_document.h"

DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);

IE_NAMESPACE_BEGIN(document);

class FieldBuilder
{
public:
    static const uint32_t MAX_SECTION_PER_FIELD = Field::MAX_SECTION_PER_FIELD;

public:
    FieldBuilder(IE_NAMESPACE(document)::IndexTokenizeField* field,
                 autil::mem_pool::Pool *pool);
    ~FieldBuilder();

public:
    const IE_NAMESPACE(document)::Token* AddToken(
            uint64_t hashKey, pospayload_t posPayload = 0);

    const IE_NAMESPACE(document)::Token* AddTokenWithPosition(
            uint64_t hashKey, pos_t pos, pospayload_t posPayload = 0);
    bool AddPlaceHolder();
    void BeginSection(section_weight_t secWeight);
    section_len_t EndSection();
    uint32_t EndField();

    void SetMaxSectionLen(section_len_t maxSectionLen)
    { 
        if (maxSectionLen == 0 || maxSectionLen > Section::MAX_SECTION_LENGTH) {
            mMaxSectionLen = Section::MAX_SECTION_LENGTH;
        } else {
            mMaxSectionLen = maxSectionLen;
        }
    }

    section_len_t GetMaxSectionLen() const
    { return mMaxSectionLen; }

    void SetMaxSectionPerField(uint32_t maxSectionPerField);

    uint32_t GetMaxSectionPerField() const
    { return mMaxSectionPerField; }

protected:
    bool GetNewSection();

private:
    IndexTokenizeField* mField;
    Section* mCurrSection;
    section_len_t mCurrSectionLen;
    section_len_t mMaxSectionLen;
    pos_t mLastTokenPos;
    pos_t mPos;
    section_weight_t mSectionWeight;
    pos_t mNextPos;
    uint32_t mMaxSectionPerField;
    autil::mem_pool::Pool *mPool;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<FieldBuilder> FieldBuilderPtr;

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_FIELD_BUILDER_H
