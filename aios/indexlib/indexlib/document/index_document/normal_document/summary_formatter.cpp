#include <memory>
#include <assert.h>
#include "indexlib/document/index_document/normal_document/summary_formatter.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SummaryFormatter);

SummaryFormatter::SummaryFormatter(const SummarySchemaPtr& summarySchema)
    : mSummarySchema(summarySchema)
{
    if (mSummarySchema && mSummarySchema->NeedStoreSummary())
    {
        for (summarygroupid_t groupId = 0;
             groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId)
        {
            const SummaryGroupConfigPtr& summaryGroupConfig =
                summarySchema->GetSummaryGroupConfig(groupId);
            if (summaryGroupConfig->NeedStoreSummary())
            {
                mGroupFormatterVec.push_back(SummaryGroupFormatterPtr(
                                new SummaryGroupFormatter(summaryGroupConfig)));
            }
            else
            {
                mGroupFormatterVec.push_back(SummaryGroupFormatterPtr());
            }
        }
        mLengthVec.resize(mGroupFormatterVec.size());
    }
}

size_t SummaryFormatter::GetSerializeLength(const SummaryDocumentPtr& document)
{
    if (mGroupFormatterVec.size() == 1)
    {
        // only default group
        assert(mGroupFormatterVec[0]);
        return mGroupFormatterVec[0]->GetSerializeLength(document);
    }
    size_t totalLength = 0;
    for (size_t i = 0; i < mGroupFormatterVec.size(); ++i)
    {
        if (!mGroupFormatterVec[i])
        {
            continue;
        }
        mLengthVec[i] = (uint32_t)mGroupFormatterVec[i]->GetSerializeLength(document);
        totalLength += SummaryGroupFormatter::GetVUInt32Length(mLengthVec[i]);
        totalLength += mLengthVec[i];
    }
    return totalLength;
}

void SummaryFormatter::SerializeSummary(const SummaryDocumentPtr& document,
                                        char* value, size_t length) const
{
    if (mGroupFormatterVec.size() == 1)
    {
        // only default group
        assert(mGroupFormatterVec[0]);
        mGroupFormatterVec[0]->SerializeSummary(document, value, length);
        return;
    }

    char* cursor = value;
    for (size_t i = 0; i < mGroupFormatterVec.size(); ++i)
    {
        if (!mGroupFormatterVec[i])
        {
            continue;
        }
        uint32_t groupLength = mLengthVec[i];
        assert(length >= (uint32_t)(cursor - value) + groupLength +
               SummaryGroupFormatter::GetVUInt32Length(groupLength));
        SummaryGroupFormatter::WriteVUInt32(groupLength, cursor);
        mGroupFormatterVec[i]->SerializeSummary(document, cursor, groupLength);
        cursor += groupLength;
    }
}

bool SummaryFormatter::TEST_DeserializeSummary(document::SearchSummaryDocument* document,
        const char* value, size_t length) const
{
    assert(mGroupFormatterVec.size() == 1 && mGroupFormatterVec[0]);
    return mGroupFormatterVec[0]->DeserializeSummary(document, value, length);
}

// summary doc -> serialized doc,
// bs processor use it serialize summary doc came from raw doc
void SummaryFormatter::SerializeSummaryDoc(const SummaryDocumentPtr& doc,
        SerializedSummaryDocumentPtr& serDoc)
{
    size_t length = GetSerializeLength(doc);
    char* value = new char[length];

    SerializeSummary(doc, value, length);
    if (!serDoc)
    {
        serDoc.reset(new SerializedSummaryDocument());
    }
    serDoc->SetSerializedDoc(value, length);
    serDoc->SetDocId(doc->GetDocId());
}

void SummaryFormatter::TEST_DeserializeSummaryDoc(const SerializedSummaryDocumentPtr& serDoc,
        SearchSummaryDocument* document)
{
    assert(document);
    TEST_DeserializeSummary(document, serDoc->GetValue(), serDoc->GetLength());
}

IE_NAMESPACE_END(document);

