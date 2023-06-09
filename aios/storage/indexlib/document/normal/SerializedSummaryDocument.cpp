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
#include "indexlib/document/normal/SerializedSummaryDocument.h"

#include <new>

using namespace std;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SerializedSummaryDocument);

SerializedSummaryDocument::SerializedSummaryDocument() : _value(NULL), _length(0), _docId(INVALID_DOCID) {}

SerializedSummaryDocument::~SerializedSummaryDocument()
{
    delete[] _value;
    _value = NULL;
}

void SerializedSummaryDocument::SetSerializedDoc(char* value, size_t length)
{
    assert(value);
    _value = value;
    _length = length;
}

void SerializedSummaryDocument::Serialize(const std::shared_ptr<SerializedSummaryDocument>& summary, std::string& str)
{
#define WRITE(s, val) s.append((const char*)&(val), sizeof(val))
    if (summary == NULL) {
        docid_t invalidDocId = INVALID_DOCID;
        size_t valueLength = 0;
        WRITE(str, invalidDocId);
        WRITE(str, valueLength);
        return;
    }

    WRITE(str, summary->_docId);
    WRITE(str, summary->_length);
    str.append((const char*)summary->_value, summary->_length);
}

std::shared_ptr<SerializedSummaryDocument> SerializedSummaryDocument::Deserialize(const string& str, uint32_t offset,
                                                                                  uint32_t& readBytes)
{
#define READ(p, val, type, msg)                                                                                        \
    {                                                                                                                  \
        if (remainBytes < sizeof(val)) {                                                                               \
            AUTIL_LOG(ERROR, msg);                                                                                     \
            return std::shared_ptr<SerializedSummaryDocument>();                                                       \
        }                                                                                                              \
        assert(sizeof(type) == sizeof(val));                                                                           \
        val = *((type*)p);                                                                                             \
        p += sizeof(val);                                                                                              \
        readBytes += sizeof(val);                                                                                      \
        remainBytes -= sizeof(val);                                                                                    \
    }

    const char* ptr = str.data() + offset;
    uint32_t remainBytes = str.size() - offset;
    readBytes = 0;

    if (remainBytes < sizeof(docid_t) + sizeof(uint32_t)) {
        return std::shared_ptr<SerializedSummaryDocument>();
    }

    std::shared_ptr<SerializedSummaryDocument> docPtr(new SerializedSummaryDocument);

    READ(ptr, docPtr->_docId, docid_t, "Deserialize DocId FAIL");
    READ(ptr, docPtr->_length, size_t, "Deserialize doc length FAIL");

    if (docPtr->_docId == INVALID_DOCID) {
        return std::shared_ptr<SerializedSummaryDocument>();
    }

    if (docPtr->_length > remainBytes) {
        AUTIL_LOG(ERROR, "Deserialize summary doc FAILED");
        return std::shared_ptr<SerializedSummaryDocument>();
    }

    char* values = new (nothrow) char[docPtr->_length];
    assert(values);
    memcpy(values, ptr, docPtr->_length);
    docPtr->_value = values;
    readBytes += docPtr->_length;
    return docPtr;
}

void SerializedSummaryDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_length);
    if (_length > 0) {
        dataBuffer.writeBytes(_value, _length);
    }
}

void SerializedSummaryDocument::deserialize(autil::DataBuffer& dataBuffer, uint32_t docVersion)
{
    dataBuffer.read(_length);
    if (_length > 0) {
        _value = new (nothrow) char[_length];
        dataBuffer.readBytes(_value, _length);
    }
}
}} // namespace indexlib::document
