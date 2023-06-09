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
#include "indexlib/document/IDocument.h"
#include "indexlib/document/normal/AttributeDocument.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SerializedSummaryDocument.h"

namespace indexlib::document {
class NormalDocument;
}

namespace indexlibv2::document {

class NormalDocument : public IDocument
{
private:
    typedef std::map<std::string, std::string> TagInfoMap;

public:
    NormalDocument();
    explicit NormalDocument(const std::shared_ptr<autil::mem_pool::Pool>& pool);
    ~NormalDocument();

public:
    typedef std::vector<fieldid_t> FieldIdVector;
    typedef std::vector<std::shared_ptr<NormalDocument>> DocumentVector;

public:
    const indexlibv2::framework::Locator& GetLocatorV2() const override { return _locatorV2; }
    DocInfo GetDocInfo() const override { return _docInfo; }
    uint32_t GetTTL() const override { return _ttl; }
    docid_t GetDocId() const override
    {
        if (_indexDocument) {
            return _indexDocument->GetDocId();
        }

        if (_attributeDocument) {
            return _attributeDocument->GetDocId();
        }

        if (_summaryDocument) {
            return _summaryDocument->GetDocId();
        }

        return INVALID_DOCID;
    }
    size_t EstimateMemory() const override;
    DocOperateType GetDocOperateType() const override { return _opType; }
    void SetDocOperateType(DocOperateType op) override
    {
        _opType = op;
        _originalOpType = op;
    }
    bool HasFormatError() const override;

public:
    Status SetSerializedVersion(uint32_t version);
    uint32_t GetSerializedVersion() const;

    void SetLocator(const indexlibv2::framework::Locator& locator) override { _locatorV2 = locator; };

    DocOperateType GetOriginalOperateType() const { return _originalOpType; }

    void SetTTL(uint32_t ttl) { _ttl = ttl; }

    void AddTag(const std::string& key, const std::string& value);
    const std::string& GetTag(const std::string& key) const;
    const std::map<std::string, std::string>& GetTagInfoMap() const { return _tagInfo; }

    bool IsUserDoc() const;
    void SetDocInfo(const DocInfo& docInfo) override { _docInfo = docInfo; }

    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override;

    void ModifyDocOperateType(DocOperateType op) { _opType = op; }
    autil::StringView GetTraceId() const override;
    void SetTrace(bool trace) override { _trace = trace; }
    bool CheckOrTrim(int64_t timestamp) { return GetDocInfo().timestamp >= timestamp; }

    void SetIngestionTimestamp(int64_t ingestionTimestamp);
    int64_t GetIngestionTimestamp() const override;

    void SetSource(const std::string& src) { AddTag(DOCUMENT_SOURCE_TAG_KEY, src); }
    autil::StringView GetSource() const override
    {
        const auto& str = GetTag(DOCUMENT_SOURCE_TAG_KEY);
        return autil::StringView(str);
    }

public:
    void SetIndexDocument(const std::shared_ptr<indexlib::document::IndexDocument>& indexDocument)
    {
        _indexDocument = indexDocument;
    }

    void SetAttributeDocument(const std::shared_ptr<indexlib::document::AttributeDocument>& attributeDocument)
    {
        _attributeDocument = attributeDocument;
    }

    void SetSummaryDocument(const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& summaryDocument)
    {
        _summaryDocument = summaryDocument;
    }

    void SetSourceDocument(const std::shared_ptr<indexlib::document::SerializedSourceDocument>& sourceDocument)
    {
        _sourceDocument = sourceDocument;
    }

    const std::shared_ptr<indexlib::document::IndexDocument>& GetIndexDocument() const { return _indexDocument; }

    const std::shared_ptr<indexlib::document::AttributeDocument>& GetAttributeDocument() const
    {
        return _attributeDocument;
    }

    const std::shared_ptr<indexlib::document::SerializedSummaryDocument>& GetSummaryDocument() const
    {
        return _summaryDocument;
    }

    const std::shared_ptr<indexlib::document::SerializedSourceDocument>& GetSourceDocument() const
    {
        return _sourceDocument;
    }

    void SetDocId(docid_t docId)
    {
        if (_indexDocument)
            _indexDocument->SetDocId(docId);
        if (_summaryDocument)
            _summaryDocument->SetDocId(docId);
        if (_attributeDocument)
            _attributeDocument->SetDocId(docId);
    }

    const std::string& GetPrimaryKey() const;

    segmentid_t GetSegmentIdBeforeModified() const { return _segmentIdBeforeModified; }

    void SetSegmentIdBeforeModified(segmentid_t segmentId) { _segmentIdBeforeModified = segmentId; }

    void SetUserTimestamp(int64_t timestamp) { _userTimestamp = timestamp; }

    int64_t GetUserTimestamp() const { return _userTimestamp; }

    bool HasPrimaryKey() const { return _indexDocument && !_indexDocument->GetPrimaryKey().empty(); }

    bool HasIndexUpdate() const;
    const FieldIdVector& GetModifiedFields() const { return _modifiedFields; }
    void AddModifiedField(fieldid_t fid) { _modifiedFields.push_back(fid); }
    void ClearModifiedFields() { _modifiedFields.clear(); }

    const FieldIdVector& GetModifyFailedFields() const { return _modifyFailedFields; }
    void AddModifyFailedField(fieldid_t fid) { _modifyFailedFields.push_back(fid); }

    bool operator==(const NormalDocument& doc) const;
    bool operator!=(const NormalDocument& doc) const { return !(*this == doc); }

    autil::mem_pool::Pool* GetPool() const { return _pool.get(); }

    size_t GetMemoryUse() const;

    std::string SerializeToString() const;
    void DeserializeFromString(const std::string& docStr);

protected:
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const;

    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion);
    std::string TagInfoToString() const;
    void TagInfoFromString(const std::string& str);
    void SimpleTagInfoDecode(const std::string& str, TagInfoMap* tagInfo);

public:
    uint32_t GetDocBinaryVersion() const { return DOCUMENT_BINARY_VERSION; }

private:
    template <typename T>
    inline T* deserializeObject(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj) {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        // T* tPtr = IE_POOL_NEW_CLASS(pool, T, pool);
        ptr.reset(new T(pool));
        ptr->deserialize(dataBuffer, pool, docVersion);
        return ptr.release();
    }

    template <typename T>
    inline T* deserializeObject(autil::DataBuffer& dataBuffer, uint32_t docVersion)
    {
        bool hasObj;
        dataBuffer.read(hasObj);
        if (!hasObj) {
            return NULL;
        }

        // deserialize may throw exception, use auto ptr
        std::unique_ptr<T> ptr;
        ptr.reset(new T());
        ptr->deserialize(dataBuffer, docVersion);
        return ptr.release();
    }

    void deserializeVersion3(autil::DataBuffer& dataBuffer);
    void deserializeVersion4(autil::DataBuffer& dataBuffer);
    void deserializeVersion5(autil::DataBuffer& dataBuffer);
    void deserializeVersion6(autil::DataBuffer& dataBuffer);
    void deserializeVersion7(autil::DataBuffer& dataBuffer);
    inline void deserializeVersion8(autil::DataBuffer& dataBuffer)
    {
        assert(_attributeDocument);
        _attributeDocument->deserializeVersion8(dataBuffer);
    }
    inline void deserializeVersion9(autil::DataBuffer& dataBuffer)
    {
        _sourceDocument.reset(
            deserializeObject<indexlib::document::SerializedSourceDocument>(dataBuffer, _pool.get(), 9));
    }
    inline void deserializeVersion10(autil::DataBuffer& dataBuffer)
    {
        if (_indexDocument) {
            _indexDocument->deserializeVersion10(dataBuffer);
        }
    }
    inline void deserializeVersion11(autil::DataBuffer& dataBuffer)
    {
        if (_indexDocument) {
            _indexDocument->deserializeVersion11(dataBuffer);
        }
    }

    void serializeVersion3(autil::DataBuffer& dataBuffer) const;
    void serializeVersion4(autil::DataBuffer& dataBuffer) const;
    void serializeVersion5(autil::DataBuffer& dataBuffer) const;
    void serializeVersion6(autil::DataBuffer& dataBuffer) const;
    void serializeVersion7(autil::DataBuffer& dataBuffer) const;
    inline void serializeVersion8(autil::DataBuffer& dataBuffer) const
    {
        assert(_attributeDocument);
        _attributeDocument->serializeVersion8(dataBuffer);
    }
    inline void serializeVersion9(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_sourceDocument); }
    inline void serializeVersion10(autil::DataBuffer& dataBuffer) const
    {
        if (_indexDocument) {
            _indexDocument->serializeVersion10(dataBuffer);
        }
    }
    inline void serializeVersion11(autil::DataBuffer& dataBuffer) const
    {
        if (_indexDocument) {
            _indexDocument->serializeVersion11(dataBuffer);
        }
    }

private:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    DocOperateType _opType = UNKNOWN_OP;
    DocOperateType _originalOpType = UNKNOWN_OP;
    mutable uint32_t _serializedVersion = INVALID_DOC_VERSION;
    uint32_t _ttl = 0;
    DocInfo _docInfo;
    TagInfoMap _tagInfo; // legacy mSource
    indexlibv2::framework::Locator _locatorV2;
    int64_t _userTimestamp = INVALID_TIMESTAMP;
    segmentid_t _segmentIdBeforeModified = INVALID_SEGMENTID;

    FieldIdVector _modifyFailedFields;
    std::shared_ptr<indexlib::document::IndexDocument> _indexDocument;
    std::shared_ptr<indexlib::document::AttributeDocument> _attributeDocument;
    std::shared_ptr<indexlib::document::SerializedSummaryDocument> _summaryDocument;
    std::shared_ptr<indexlib::document::SerializedSourceDocument> _sourceDocument;
    FieldIdVector _modifiedFields;

    bool _trace = false; // used for doc trace

private:
    friend class indexlib::document::NormalDocument;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
