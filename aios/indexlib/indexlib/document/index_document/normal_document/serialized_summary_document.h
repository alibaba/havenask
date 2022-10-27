#ifndef __INDEXLIB_SERIALIZED_SUMMARY_DOCUMENT_H
#define __INDEXLIB_SERIALIZED_SUMMARY_DOCUMENT_H

#include <tr1/memory>
#include <autil/DataBuffer.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(document, SerializedSummaryDocument);

IE_NAMESPACE_BEGIN(document);

class SerializedSummaryDocument
{
public:
    SerializedSummaryDocument();
    ~SerializedSummaryDocument();
public:
    void SetDocId(docid_t docId) { mDocId = docId; }
    docid_t GetDocId() const { return mDocId; }
    void SetSerializedDoc(char* value, size_t length);
    size_t GetLength() const { return mLength; }
    const char* GetValue() const { return mValue; }

    bool operator==(const SerializedSummaryDocument& doc)
    {
        if (mLength != doc.mLength || mDocId != doc.mDocId)
        {
            return false;
        }
        if ((mValue == NULL && doc.mValue != NULL) ||
            (mValue != NULL && doc.mValue == NULL))
        {
            return false;
        }
        if (mValue != NULL && doc.mValue != NULL)
        {
            return memcmp(mValue, doc.mValue, mLength) == 0;
        }
        return true; // mValue == NULL && doc.mValue == NULL
    }
    bool operator!=(const SerializedSummaryDocument& doc)
    {
        return !operator==(doc);
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    
    void deserialize(autil::DataBuffer &dataBuffer,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);

    static void Serialize(const SerializedSummaryDocumentPtr& summary, std::string& str);
    
    static SerializedSummaryDocumentPtr Deserialize(const std::string& str, 
            uint32_t offset, uint32_t& readBytes);

private:
    char* mValue;
    size_t mLength;
    docid_t mDocId;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SerializedSummaryDocument);
///////////////////////////////////////
IE_NAMESPACE_END(document);

#endif //__INDEXLIB_SERIALIZED_SUMMARY_DOCUMENT_H
