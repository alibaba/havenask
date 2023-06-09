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
#pragma once
#include <memory>

#include "autil/DataBuffer.h"
#include "indexlib/base/Constant.h"

namespace indexlib { namespace document {

class SerializedSummaryDocument
{
public:
    SerializedSummaryDocument();
    ~SerializedSummaryDocument();

public:
    void SetDocId(docid_t docId) { _docId = docId; }
    docid_t GetDocId() const { return _docId; }
    void SetSerializedDoc(char* value, size_t length);
    size_t GetLength() const { return _length; }
    const char* GetValue() const { return _value; }

    bool operator==(const SerializedSummaryDocument& doc) const
    {
        if (_length != doc._length || _docId != doc._docId) {
            return false;
        }
        if ((_value == NULL && doc._value != NULL) || (_value != NULL && doc._value == NULL)) {
            return false;
        }
        if (_value != NULL && doc._value != NULL) {
            return memcmp(_value, doc._value, _length) == 0;
        }
        return true; // _value == NULL && doc._value == NULL
    }
    bool operator!=(const SerializedSummaryDocument& doc) const { return !operator==(doc); }

    void serialize(autil::DataBuffer& dataBuffer) const;

    void deserialize(autil::DataBuffer& dataBuffer, uint32_t docVersion = DOCUMENT_BINARY_VERSION);

    static void Serialize(const std::shared_ptr<SerializedSummaryDocument>& summary, std::string& str);

    static std::shared_ptr<SerializedSummaryDocument> Deserialize(const std::string& str, uint32_t offset,
                                                                  uint32_t& readBytes);

private:
    char* _value;
    size_t _length;
    docid_t _docId;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
