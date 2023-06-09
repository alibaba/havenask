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

#include "indexlib/base/Constant.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/raw_document/DefaultRawDocFieldIterator.h"
#include "indexlib/document/raw_document/KeyMapManager.h"
#include "indexlib/framework/Locator.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2 { namespace document {

class DefaultRawDocument : public RawDocument
{
public:
    typedef DefaultRawDocFieldIterator::FieldVec FieldVec;
    typedef FieldVec::const_iterator Iterator;

    static std::string RAW_DOC_IDENTIFIER;

public:
    // Default Constructor is not recommended and inefficient.
    // Because it will create a hash map manager instead of sharing one.
    DefaultRawDocument()
        : RawDocument()
        , _hashMapManager(new KeyMapManager())
        , _hashMapIncrement(new KeyMap())
        , _fieldCount(0)
    {
        _hashMapPrimary = _hashMapManager->getHashMapPrimary();
    }

    DefaultRawDocument(const std::shared_ptr<KeyMapManager>& hashMapManager)
        : RawDocument()
        , _hashMapManager(hashMapManager)
        , _hashMapIncrement(new KeyMap())
        , _fieldCount(0)
    {
        if (_hashMapManager == NULL) {
            _hashMapManager.reset(new KeyMapManager());
        }
        _hashMapPrimary = _hashMapManager->getHashMapPrimary();
        if (_hashMapPrimary) {
            _fieldsPrimary.resize(_hashMapPrimary->size());
        }
    }

    DefaultRawDocument(const DefaultRawDocument& other);

    virtual ~DefaultRawDocument() { _hashMapManager->updatePrimary(_hashMapIncrement); }

public:
    bool Init(const std::shared_ptr<DocumentInitParam>& initParam) override { return true; }

    void setFieldNoCopy(const autil::StringView& fieldName, const autil::StringView& fieldValue) override;

    void setField(const autil::StringView& fieldName, const autil::StringView& fieldValue) override;

    const autil::StringView& getField(const autil::StringView& fieldName) const override;

    bool exist(const autil::StringView& fieldName) const override;

    void eraseField(const autil::StringView& fieldName) override;

    uint32_t getFieldCount() const override;

    void clear() override;

    DefaultRawDocument* clone() const override;

    DefaultRawDocument* createNewDocument() const override;

    DocOperateType getDocOperateType() const override;

    std::string getIdentifier() const override { return RAW_DOC_IDENTIFIER; }

    virtual RawDocFieldIterator* CreateIterator() const override;

    void extractFieldNames(std::vector<autil::StringView>& fieldNames) const override;

    int64_t GetUserTimestamp() const override { return INVALID_TIMESTAMP; }

    void setDocOperateType(DocOperateType type) override { _opType = type; }

    int64_t getDocTimestamp() const override { return _timestamp; }
    void setDocTimestamp(int64_t timestamp) override { _timestamp = timestamp; }

    void AddTag(const std::string& key, const std::string& value) override;
    const std::string& GetTag(const std::string& key) const override;
    const std::map<std::string, std::string>& GetTagInfoMap() const override { return _tagInfo; }
    const framework::Locator& getLocator() const override { return _locator; }
    void SetLocator(const framework::Locator& locator) override { _locator = locator; }
    void SetIngestionTimestamp(int64_t ingestionTimestamp) override;
    int64_t GetIngestionTimestamp() const override;
    void SetDocInfo(const indexlibv2::document::IDocument::DocInfo& docInfo) override;
    indexlibv2::document::IDocument::DocInfo GetDocInfo() const override;
    size_t EstimateMemory() const override;

public:
    using RawDocument::eraseField;
    using RawDocument::exist;
    using RawDocument::getField;
    using RawDocument::setField;

    std::shared_ptr<KeyMapManager> getHashMapManager() const { return _hashMapManager; }

private:
    void addNewField(const autil::StringView& fieldName, const autil::StringView& fieldValue);
    const autil::StringView* search(const autil::StringView& fieldName) const;
    autil::StringView* search(const autil::StringView& fieldName);

public:
    // for log out
    std::string toString() const override;

private:
    // to manager the version of primary hash map.
    std::shared_ptr<KeyMapManager> _hashMapManager;
    // hash map: keep the map from key to id.
    // primary: A sharing and Read-Only hash map among documents.
    std::shared_ptr<KeyMap> _hashMapPrimary;
    // increment: A private and incremental hash map.
    std::shared_ptr<KeyMap> _hashMapIncrement;
    // fields: keep the map from id to value.
    FieldVec _fieldsPrimary;
    FieldVec _fieldsIncrement;
    uint32_t _fieldCount;
    mutable DocOperateType _opType = UNKNOWN_OP;
    int64_t _timestamp = INVALID_TIMESTAMP;
    TagInfoMap _tagInfo; // legacy mSource
    framework::Locator _locator;
    indexlibv2::document::IDocument::DocInfo _docInfo;

    static const autil::StringView EMPTY_STRING;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlibv2::document
