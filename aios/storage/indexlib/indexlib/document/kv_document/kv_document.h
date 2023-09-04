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
#ifndef __INDEXLIB_KV_DOCUMENT_H
#define __INDEXLIB_KV_DOCUMENT_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/common_define.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/document/raw_document/key_map_manager.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class KVDocument : public document::RawDocument, public Document
{
public:
    KVDocument();
    KVDocument(const document::KeyMapManagerPtr& hashMapManager);
    ~KVDocument();

    using DocAllocator = autil::mem_pool::pool_allocator<KVIndexDocument>;
    using DocVector = std::vector<KVIndexDocument, DocAllocator>;

    KVDocument(const KVDocument&) = delete;
    KVDocument& operator=(const KVDocument&) = delete;
    KVDocument(KVDocument&&);
    KVDocument& operator=(KVDocument&&);

public:
    // Document

    void SetDocOperateType(DocOperateType op) override { setDocOperateType(op); }
    void ModifyDocOperateType(DocOperateType op) override { SetDocOperateType(op); }
    uint32_t GetDocBinaryVersion() const override { return KVIndexDocument::STORE_TAG_INFO_MAP_VERSION; }
    bool HasFormatError() const override { return _hasFormatError; }
    void SetDocId(docid_t docId) override {}
    docid_t GetDocId() const override { return INVALID_DOCID; }

    size_t GetMessageCount() const override
    {
        // we think @_rawDoc as one message, which maybe exist in @_doc
        size_t baseDocCount = _rawDoc ? 1 : 0;
        return std::max(baseDocCount, _docs.size());
    }

    size_t GetDocumentCount() const override { return _docs.size(); }
    size_t EstimateMemory() const override;
    bool CheckOrTrim(int64_t timestamp) override;

public:
    // RawDocument
    void setDocOperateType(DocOperateType op) override
    {
        if (_rawDoc) {
            _rawDoc->setDocOperateType(op);
        }
        Document::SetDocOperateType(op);
    }
    DocOperateType getDocOperateType() const override
    {
        DocOperateType op = UNKNOWN_OP;
        if (_rawDoc) {
            op = _rawDoc->getDocOperateType();
        }
        if (op != UNKNOWN_OP) {
            return op;
        }
        return Document::GetDocOperateType();
    }
    bool Init(const document::DocumentInitParamPtr& initParam) override;
    void setField(const autil::StringView& fieldName, const autil::StringView& fieldValue) override;
    void setFieldNoCopy(const autil::StringView& fieldName, const autil::StringView& fieldValue) override;
    const autil::StringView& getField(const autil::StringView& fieldName) const override;
    RawDocFieldIterator* CreateIterator() const override;
    bool exist(const autil::StringView& fieldName) const override;
    void eraseField(const autil::StringView& fieldName) override;
    uint32_t getFieldCount() const override;
    void clear() override;
    KVDocument* clone() const override;
    KVDocument* createNewDocument() const override;
    std::string getIdentifier() const override;
    std::string toString() const override;
    int64_t GetUserTimestamp() const override;
    void extractFieldNames(std::vector<autil::StringView>& fieldNames) const override;
    int64_t getDocTimestamp() const override { return GetTimestamp(); }
    void setDocTimestamp(int64_t timestamp) override { SetTimestamp(timestamp); }
    void AddTag(const std::string& key, const std::string& value) override { return Document::AddTag(key, value); }
    const std::string& GetTag(const std::string& key) const override { return Document::GetTag(key); }
    const std::map<std::string, std::string>& GetTagInfoMap() const override { return Document::GetTagInfoMap(); }
    const indexlibv2::framework::Locator& getLocator() const override { return Document::GetLocatorV2(); }
    void SetLocator(const indexlibv2::framework::Locator& locator) override
    {
        if (locator == getLocator()) {
            return;
        }
        Document::SetLocator(locator);
    }
    void SetIngestionTimestamp(int64_t ingestionTimestamp) override
    {
        Document::SetIngestionTimestamp(ingestionTimestamp);
    }
    int64_t GetIngestionTimestamp() const override { return Document::GetIngestionTimestamp(); }
    void SetDocInfo(const indexlibv2::document::IDocument::DocInfo& docInfo) override { Document::SetDocInfo(docInfo); }
    indexlibv2::document::IDocument::DocInfo GetDocInfo() const override { return Document::GetDocInfo(); }

public:
    DocVector::iterator begin() { return _docs.begin(); }
    DocVector::iterator end() { return _docs.end(); }
    KVIndexDocument& back() { return _docs.back(); }

public:
    KVIndexDocument* CreateOneDoc();
    void EraseLastDoc();
    void ReleaseRawDoc() { _rawDoc.reset(); }
    void MergeRawDoc(KVDocument* other)
    {
        assert(other);
        _rawDoc = std::move(other->_rawDoc);
    }

    void EnsureRawDoc()
    {
        if (!_rawDoc) {
            _rawDoc = std::make_unique<document::DefaultRawDocument>(_keyMapManager);
            _rawDoc->setDocOperateType(Document::GetDocOperateType());
        }
    }
    void SetFormatError() { _hasFormatError = true; }
    void ClearFormatError() { _hasFormatError = false; }
    const document::KeyMapManagerPtr& GetKeyMapManager() const { return _keyMapManager; }

    KVDocument* CloneKVDocument() const;

    bool NeedDeserializeFromLegacy() const { return _needDeserializeFromLegacy; }

    void SetNeedStorePKeyValue(bool needStorePKeyValue) { _needStorePKeyValue = needStorePKeyValue; }

private:
    // Document
    void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const override;
    void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) override;

private:
    std::unique_ptr<autil::mem_pool::UnsafePool> _unsafePool;
    DocVector _docs;
    bool _hasFormatError;
    document::KeyMapManagerPtr _keyMapManager;
    std::unique_ptr<document::DefaultRawDocument> _rawDoc;
    bool _needDeserializeFromLegacy;
    bool _needStorePKeyValue = false;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVDocument);

}} // namespace indexlib::document

#endif //__INDEXLIB_KV_DOCUMENT_H
