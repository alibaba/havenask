#include <new>
#include "indexlib/document/index_document/normal_document/serialized_summary_document.h"

using namespace std;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, SerializedSummaryDocument);

SerializedSummaryDocument::SerializedSummaryDocument()
    : mValue(NULL)
    , mLength(0)
    , mDocId(INVALID_DOCID)
{
}

SerializedSummaryDocument::~SerializedSummaryDocument()
{
    delete []mValue;
    mValue = NULL;
}

void SerializedSummaryDocument::SetSerializedDoc(char* value, size_t length)
{
    assert(value);
    mValue = value;
    mLength = length;
}

void SerializedSummaryDocument::Serialize(
        const SerializedSummaryDocumentPtr& summary, std::string& str)
{
#define WRITE(s,val) s.append((const char*)&(val), sizeof(val))
    if (summary == NULL)
    {
        docid_t invalidDocId = INVALID_DOCID;
        size_t valueLength = 0;
        WRITE(str, invalidDocId);
        WRITE(str, valueLength);
        return;
    }

    WRITE(str, summary->mDocId);
    WRITE(str, summary->mLength);
    str.append((const char*)summary->mValue, summary->mLength);
}

SerializedSummaryDocumentPtr SerializedSummaryDocument::Deserialize(
        const string& str, uint32_t offset, uint32_t& readBytes)
{
#define READ(p,val,type,msg)  {                         \
        if (remainBytes < sizeof(val))                  \
        {                                               \
            IE_LOG(ERROR, msg);                         \
            return SerializedSummaryDocumentPtr();      \
        }                                               \
        assert(sizeof(type) == sizeof(val));            \
        val = *((type*)p);                              \
        p += sizeof(val);                               \
        readBytes += sizeof(val);                       \
        remainBytes -= sizeof(val);                     \
    }

    const char* ptr = str.data() + offset;
    uint32_t remainBytes = str.size() - offset;
    readBytes = 0;

    if (remainBytes < sizeof(docid_t) + sizeof(uint32_t))
    {
        return SerializedSummaryDocumentPtr();
    }

    SerializedSummaryDocumentPtr docPtr(new SerializedSummaryDocument);

    READ(ptr, docPtr->mDocId, docid_t, "Deserialize DocId FAIL");
    READ(ptr, docPtr->mLength, size_t, "Deserialize doc length FAIL");

    if (docPtr->mDocId == INVALID_DOCID)
    {
        return SerializedSummaryDocumentPtr();
    }

    if (docPtr->mLength > remainBytes)
    {
        IE_LOG(ERROR, "Deserialize summary doc FAILED");
        return SerializedSummaryDocumentPtr();
    }

    char* values = new (nothrow) char[docPtr->mLength];
    assert(values);
    memcpy(values, ptr, docPtr->mLength);
    docPtr->mValue = values;
    readBytes += docPtr->mLength;
    return docPtr;
}


void SerializedSummaryDocument::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(mLength);
    if (mLength > 0)
    {
        dataBuffer.writeBytes(mValue, mLength);
    }
}

void SerializedSummaryDocument::deserialize(autil::DataBuffer &dataBuffer, uint32_t docVersion) {
    dataBuffer.read(mLength);
    if (mLength > 0)
    {
        mValue = new  (nothrow) char[mLength];
        assert(mValue);
        dataBuffer.readBytes(mValue, mLength);
    }
}

IE_NAMESPACE_END(document);
