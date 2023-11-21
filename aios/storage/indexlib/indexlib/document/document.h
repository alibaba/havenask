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

#include <functional>
#include <memory>

#include "autil/DataBuffer.h"
#include "indexlib/common_define.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::document {
class RawDocument;
}

namespace indexlib { namespace document {

class Document : public indexlibv2::document::IDocument
{
private:
    typedef std::map<std::string, std::string> TagInfoMap;

public:
    Document();
    Document(const Document& other);
    virtual ~Document();

public:
    bool operator==(const Document& doc) const;
    bool operator!=(const Document& doc) const { return !(*this == doc); }

    void SetSerializedVersion(uint32_t version);
    uint32_t GetSerializedVersion() const;

    const Locator& GetLocator() const { return mLocator; }
    const indexlibv2::framework::Locator& GetLocatorV2() const override { return mLocatorV2; }
    void ResetLocator()
    {
        mLocator = indexlib::document::Locator();
        mLocatorV2 = indexlibv2::framework::Locator();
    }
    void SetLocator(const indexlib::document::IndexLocator& indexLocator);
    void SetLocator(const indexlibv2::framework::Locator& locator) override;

    void SetTimestamp(int64_t timestamp) { mTimestamp = timestamp; }
    int64_t GetTimestamp() const { return mTimestamp; }

    DocOperateType GetDocOperateType() const override { return mOpType; }
    DocOperateType GetOriginalOperateType() const { return mOriginalOpType; }

    void SetTTL(uint32_t ttl) { mTTL = ttl; }
    uint32_t GetTTL() const override { return mTTL; }

    void SetSource(const std::string& src) { AddTag(DOCUMENT_SOURCE_TAG_KEY, src); }
    autil::StringView GetSource() const override { return autil::StringView(GetTag(DOCUMENT_SOURCE_TAG_KEY)); }

    void AddTag(const std::string& key, const std::string& value);
    const std::string& GetTag(const std::string& key) const;
    const std::map<std::string, std::string>& GetTagInfoMap() const { return mTagInfo; }

    bool IsUserDoc() const;
    void SetIngestionTimestamp(int64_t ingestionTimestamp)
    {
        AddTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY, std::to_string(ingestionTimestamp));
    }
    int64_t GetIngestionTimestamp() const override
    {
        const auto& ingestionTimestampStr = GetTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY);
        if (ingestionTimestampStr.empty()) {
            return INVALID_TIMESTAMP;
        }
        return autil::StringUtil::fromString<int64_t>(ingestionTimestampStr);
    }
    void SetDocInfo(const indexlibv2::framework::Locator::DocInfo& docInfo) override { mDocInfo = docInfo; }
    indexlibv2::framework::Locator::DocInfo GetDocInfo() const override { return mDocInfo; }
    bool HasFormatError() const override { return false; }
    schemaid_t GetSchemaId() const override
    {
        assert(false);
        return DEFAULT_SCHEMAID;
    }

public:
    virtual void serialize(autil::DataBuffer& dataBuffer) const override;
    virtual void deserialize(autil::DataBuffer& dataBuffer) override;

public:
    virtual void SetDocOperateType(DocOperateType op) override
    {
        mOpType = op;
        mOriginalOpType = op;
    }
    virtual void ModifyDocOperateType(DocOperateType op) { mOpType = op; }

    virtual uint32_t GetDocBinaryVersion() const = 0;
    virtual void SetDocId(docid_t docId) = 0;
    virtual docid_t GetDocId() const override { return INVALID_DOCID; }

    // if no primary key, return empty string
    virtual const std::string& GetPrimaryKey() const;
    bool NeedTrace() const { return mTrace; }
    void SetTrace(bool trace) override { mTrace = trace; }
    autil::StringView GetTraceId() const override
    {
        if (!NeedTrace()) {
            return autil::StringView::empty_instance();
        }
        return GetPrimaryKey();
    }

    // only for parse message count, used for process step
    // TODO (yiping.typ) remove this method, and raw document parser should do it
    // but user can write user-defined parser, means it hard to change parser's interface
    virtual size_t GetMessageCount() const { return 1u; }

    // the document count in raw doc, only some special raw doc has multi doc, used in build/merge step
    virtual size_t GetDocumentCount() const { return 1u; }
    virtual size_t EstimateMemory() const override { return sizeof(Document); }
    virtual bool CheckOrTrim(int64_t timestamp) { return GetTimestamp() >= timestamp; }

    void ToRawDocument(indexlibv2::document::RawDocument* rawDoc);

protected:
    virtual void DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const = 0;

    virtual void DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) = 0;

    std::string TagInfoToString() const;
    void TagInfoFromString(const std::string& str);
    void SimpleTagInfoDecode(const std::string& str, TagInfoMap* tagInfo);

protected:
    DocOperateType mOpType;
    DocOperateType mOriginalOpType;
    mutable uint32_t mSerializedVersion;
    uint32_t mTTL;
    int64_t mTimestamp;
    indexlibv2::framework::Locator::DocInfo mDocInfo;
    TagInfoMap mTagInfo; // legacy mSource
    Locator mLocator;
    indexlibv2::framework::Locator mLocatorV2;
    bool mTrace; // used for doc trace

private:
    friend class DocumentTest;
};

DEFINE_SHARED_PTR(Document);
}} // namespace indexlib::document
