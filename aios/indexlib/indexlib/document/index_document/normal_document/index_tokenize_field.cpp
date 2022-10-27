#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, IndexTokenizeField);

IndexTokenizeField::IndexTokenizeField(autil::mem_pool::Pool* pool)
    : Field(pool, FieldTag::TOKEN_FIELD)
    , mSelfPool(1024)
    , mSections(Alloc(pool == NULL ? &mSelfPool : pool))
    , mSectionUsed(0)
{
}

IndexTokenizeField::~IndexTokenizeField() 
{
    size_t size = mSections.size();
    for ( size_t i = 0; i < size; i++ )
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mPool, mSections[i]);
        mSections[i] = NULL;
    }
    mSections.clear();    
}

void IndexTokenizeField::Reset()
{
    mSectionUsed = 0;
    size_t size = mSections.size();
    for ( size_t i = 0; i < size; i++ )
    {
        mSections[i]->Reset();
    }
    mFieldId = INVALID_FIELDID;
}

void IndexTokenizeField::serialize(autil::DataBuffer &dataBuffer) const
{
    uint32_t size = mSections.size();
    dataBuffer.write(size);
    for (size_t i = 0; i < size; ++i)
    {
        bool isNull = mSections[i];
        dataBuffer.write(isNull);
        if (isNull)
        {
            dataBuffer.write(*(mSections[i]));
        }
    }
    dataBuffer.write(mFieldId);
    dataBuffer.write(mSectionUsed);
}

void IndexTokenizeField::deserialize(autil::DataBuffer &dataBuffer)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    mSections.resize(size);
    for (uint32_t i = 0; i < size; ++i)
    {
        bool isNull;
        dataBuffer.read(isNull);
        if (isNull)
        {
            Section* pSection = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, Section,
                    Section::DEFAULT_TOKEN_NUM, mPool);
            dataBuffer.read(*pSection);
            mSections[i] = pSection;
        }
        else
        {
            mSections[i] = NULL;
        }
    }
    dataBuffer.read(mFieldId);
    dataBuffer.read(mSectionUsed);
}

Section* IndexTokenizeField::CreateSection(uint32_t tokenCount)
{
    if (mSectionUsed < MAX_SECTION_PER_FIELD)
    {
        if ( mSections.size() > mSectionUsed )
        {
            return mSections[mSectionUsed++];
        }
        else
        {
            Section* pSection = IE_POOL_COMPATIBLE_NEW_CLASS(mPool, Section, tokenCount);
            pSection->SetSectionId(mSectionUsed++);
            mSections.push_back(pSection);
            return pSection;
        }
    }
    return NULL;
}

void IndexTokenizeField::AddSection(Section* section)
{
    assert(section);
    // TODO: FIXME: to uncomment
    // if (mSectionUsed >= MAX_SECTION_PER_FIELD)
    // {
    //     INDEXLIB_THROW(misc::OutOfRangeException,
    //                  "Number of section per field exceed 256");
    // }
    section->SetSectionId(mSections.size());
    mSections.push_back(section);
    mSectionUsed++;
}

Section* IndexTokenizeField::GetSection(sectionid_t sectionId) const
{
    if ( sectionId >= mSectionUsed )
    {
        return NULL;
    }
    return mSections[sectionId];
}

bool IndexTokenizeField::operator==(const Field& field) const
{
    if (!Field::operator == (field)) {
        return false;
    }

    const IndexTokenizeField* typedOtherField =
        dynamic_cast<const IndexTokenizeField*>(&field);
    if (typedOtherField == nullptr) {
        return false;
    }
    
    if (mSectionUsed != typedOtherField->mSectionUsed)
        return false;
    
    for (uint32_t i = 0; i < mSectionUsed; ++i)
    {
        if (*mSections[i] != *(typedOtherField->mSections[i]))
            return false;
    }
    return true;
}


IE_NAMESPACE_END(document);

