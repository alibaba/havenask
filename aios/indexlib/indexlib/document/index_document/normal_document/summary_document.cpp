#include "indexlib/document/index_document/normal_document/summary_document.h"
#include <iostream>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SummaryDocument);

const ConstString& SummaryDocument::GetField(fieldid_t id) const
{
    if (id != INVALID_FIELDID && id < (fieldid_t)mFields.size())
    {
        return mFields[id];
    }
    return ConstString::EMPTY_STRING;
}

bool SummaryDocument::operator==(const SummaryDocument& right) const
{
    return mFields == right.mFields
        && mNotEmptyFieldCount == right.mNotEmptyFieldCount
        && mFirstNotEmptyFieldId == right.mFirstNotEmptyFieldId
        && mDocId == right.mDocId;
}

bool SummaryDocument::operator!=(const SummaryDocument& right) const
{
    return !operator==(right);
}

void SummaryDocument::ClearFields(const vector<fieldid_t>& fieldIds)
{
    for (size_t i = 0; i < fieldIds.size(); ++i)
    {
        fieldid_t fid = fieldIds[i];
        if ((size_t)fid < mFields.size())
        {
            mFields[fid] = ConstString::EMPTY_STRING;
        }
    }
    mNotEmptyFieldCount = 0;
    mFirstNotEmptyFieldId = INVALID_FIELDID;

    for (size_t i = 0; i < mFields.size(); ++i)
    {
        if (mFields[i].empty())
        {
            continue;
        }
        ++mNotEmptyFieldCount;
        if (mFirstNotEmptyFieldId == INVALID_FIELDID)
        {
            mFirstNotEmptyFieldId = i;
        }
    }
    if (mNotEmptyFieldCount == 0)
    {
        Reset();
    }
}

void SummaryDocument::serialize(DataBuffer &dataBuffer) const 
{
    bool partialSerialize = NeedPartialSerialize();
    dataBuffer.write(partialSerialize);

    if (!partialSerialize)
    {
        // all fields serialize
        dataBuffer.write(mFields);
        return;
    }

    // partial serialize
    dataBuffer.write(mNotEmptyFieldCount);
    for (uint32_t i = mFields.size(); i > 0; --i)
    {
        // reverse sequence, deserialize resize mFields once
        uint32_t fieldId = i - 1;
        const ConstString& value = mFields[fieldId];
        if (partialSerialize)
        {
            if (value.empty())
            {
                continue;
            }
            dataBuffer.write(fieldId);
        }
        dataBuffer.write(value);
    }
}

void SummaryDocument::deserialize(DataBuffer &dataBuffer, Pool *pool)
{
    Reset();
    bool partialSerialize;
    dataBuffer.read(partialSerialize);

    if (partialSerialize)
    {
        DeserializeNotEmptyFields(dataBuffer, pool);
    }
    else
    {
        DeserializeAllFields(dataBuffer, pool);
    }
}

void SummaryDocument::deserializeLegacyFormat(
        DataBuffer &dataBuffer, Pool *pool, uint32_t docVersion)
{
    deserialize(dataBuffer, pool);
}

void SummaryDocument::DeserializeAllFields(
        DataBuffer &dataBuffer, Pool *pool)
{
    uint32_t fieldCount;
    dataBuffer.read(fieldCount);
    mFields.resize(fieldCount);

    uint32_t emptyFieldCount = 0;
    for (uint32_t i = 0; i < fieldCount; ++i)
    {
        ConstString value;
        dataBuffer.read(value, pool);
        mFields[i] = value;

        if (value.empty())
        {
            ++emptyFieldCount;
        }
        else if (mFirstNotEmptyFieldId == INVALID_FIELDID)
        {
            mFirstNotEmptyFieldId = (fieldid_t)i;
        }
    }
    mNotEmptyFieldCount = fieldCount - emptyFieldCount;
}

void SummaryDocument::DeserializeNotEmptyFields(
        DataBuffer &dataBuffer, Pool *pool)
{
    uint32_t fieldCount;
    dataBuffer.read(fieldCount);

    for (uint32_t i = fieldCount; i > 0; --i)
    {
        uint32_t fieldId = INVALID_FIELDID;
        dataBuffer.read(fieldId);
        if (fieldId >= (uint32_t)mFields.size())
        {
            mFields.resize(fieldId + 1);
        }

        ConstString value;
        dataBuffer.read(value, pool);
        mFields[fieldId] = value;

        if (!value.empty())
        {
            mFirstNotEmptyFieldId = fieldId;
        }
    }
    mNotEmptyFieldCount = fieldCount;
}

IE_NAMESPACE_END(document);

