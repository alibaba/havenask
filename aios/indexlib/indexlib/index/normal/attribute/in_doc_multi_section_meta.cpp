#include "indexlib/index/normal/attribute/in_doc_multi_section_meta.h"
#include "indexlib/index/normal/attribute/accessor/section_attribute_reader_impl.h"

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, InDocMultiSectionMeta);

bool InDocMultiSectionMeta::UnpackInDocBuffer(
        const SectionAttributeReaderImpl* reader,
        docid_t docId)
{
    assert(reader);
    assert(reader->HasFieldId() == 
           mIndexConfig->GetSectionAttributeConfig()->HasFieldId());

    assert(reader->HasSectionWeight() == 
           mIndexConfig->GetSectionAttributeConfig()->HasSectionWeight());

    if (reader->Read(docId, mDataBuf, MAX_SECTION_BUFFER_LEN) < 0)
    {
        return false;
    }

    Init(mDataBuf, reader->HasFieldId(), reader->HasSectionWeight());
    return true;
}

section_len_t InDocMultiSectionMeta::GetSectionLen(int32_t fieldPosition,
        sectionid_t sectId) const
{
    if (mFieldPosition2Offset.empty() && GetSectionCount() > 0)
    {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)mFieldPosition2Offset.size())
    {
        uint32_t sectionCount = mFieldPosition2SectionCount[fieldPosition];
        if ((uint32_t)sectId < sectionCount)
        {
            return MultiSectionMeta::GetSectionLen(sectId + 
                    mFieldPosition2Offset[fieldPosition]);
        }
    }
    return 0;    
}

section_len_t InDocMultiSectionMeta::GetSectionLenByFieldId(fieldid_t fieldId,
        sectionid_t sectId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetSectionLen(fieldPosition, sectId);
}

section_weight_t InDocMultiSectionMeta::GetSectionWeight(int32_t fieldPosition,
        sectionid_t sectId) const
{
    if (mFieldPosition2Offset.empty() && GetSectionCount() > 0)
    {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)mFieldPosition2Offset.size())
    {
        uint32_t sectionCount = mFieldPosition2SectionCount[fieldPosition];
        if ((uint32_t)sectId < sectionCount)
        {
            return MultiSectionMeta::GetSectionWeight(sectId  + 
                    mFieldPosition2Offset[fieldPosition]);
        }
    }
    return 0;
}

section_weight_t InDocMultiSectionMeta::GetSectionWeightByFieldId(fieldid_t fieldId,
        sectionid_t sectId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetSectionWeight(fieldPosition, sectId);
}

field_len_t InDocMultiSectionMeta::GetFieldLen(int32_t fieldPosition) const
{
    if (mFieldPosition2Offset.empty() && GetSectionCount() > 0)
    {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)mFieldPosition2FieldLen.size())
    {
        return mFieldPosition2FieldLen[fieldPosition];
    }
    return 0;    
}
 
field_len_t InDocMultiSectionMeta::GetFieldLenByFieldId(fieldid_t fieldId) const
{
    int32_t fieldPosition = GetFieldIdxInPack(fieldId);
    return GetFieldLen(fieldPosition);
}

void InDocMultiSectionMeta::InitCache() const
{
    fieldid_t maxFieldId = 0;
    for (int32_t i = 0; i < (int32_t)GetSectionCount(); ++i)
    {
        fieldid_t fieldId = MultiSectionMeta::GetFieldId(i);
        if (maxFieldId < fieldId)
        {
            maxFieldId = fieldId;
        }
    }

    mFieldPosition2Offset.resize(maxFieldId + 1, -1);
    mFieldPosition2SectionCount.resize(maxFieldId + 1, 0);
    mFieldPosition2FieldLen.resize(maxFieldId + 1, 0);

    for (int32_t i = 0; i < (int32_t)GetSectionCount(); ++i)
    {
        fieldid_t fieldId = MultiSectionMeta::GetFieldId(i);
        if (-1 == mFieldPosition2Offset[(uint8_t)fieldId])
        { 
            mFieldPosition2Offset[(uint8_t)fieldId] = i;
        }

        section_len_t length = MultiSectionMeta::GetSectionLen(i);
        mFieldPosition2FieldLen[(uint8_t)fieldId] += length;
        mFieldPosition2SectionCount[(uint8_t)fieldId] ++;
    }
}

void InDocMultiSectionMeta::GetSectionMeta(uint32_t idx, 
        section_weight_t& sectionWeight,
        int32_t& fieldPosition,
        section_len_t& sectionLength) const
{
    if (idx < GetSectionCount())
    {
        sectionWeight = MultiSectionMeta::GetSectionWeight(idx);
        fieldPosition = MultiSectionMeta::GetFieldId(idx);
        sectionLength = MultiSectionMeta::GetSectionLen(idx);
    }
}

uint32_t InDocMultiSectionMeta::GetSectionCountInField(int32_t fieldPosition) const
{
    if (mFieldPosition2Offset.empty() && GetSectionCount() > 0)
    {
        InitCache();
    }

    if (fieldPosition >= 0 && fieldPosition < (int32_t)mFieldPosition2FieldLen.size())
    {
        return mFieldPosition2SectionCount[fieldPosition];
    }
    return 0;    
}

IE_NAMESPACE_END(index);

