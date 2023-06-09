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
#ifndef ISEARCH_BS_BS_PROCESSEDDOCUMENT_H
#define ISEARCH_BS_BS_PROCESSEDDOCUMENT_H

#include "build_service/common/Locator.h"
#include "build_service/common_define.h"
#include "build_service/document/RawDocument.h"
#include "build_service/util/Log.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/document.h"
#include "indexlib/document/kv/KVDocumentBatch.h"

namespace build_service { namespace document {

class ProcessedDocument
{
public:
    enum SerializeFormat {
        SF_BINARY = 0,
        SF_RAW = 1,
    };

public:
    struct DocClusterMeta {
        DocClusterMeta()
        {
            hashId = 0;
            buildType = 0;
        }
        ~DocClusterMeta() {}
        std::string clusterName;
        hashid_t hashId;
        // '1' bit will be filter, '0' bit will be read
        uint8_t buildType;
    };
    static const uint8_t SWIFT_FILTER_BIT_REALTIME = 1;
    static const uint8_t SWIFT_FILTER_BIT_FASTQUEUE = 2;
    static const uint8_t SWIFT_FILTER_BIT_ALL = 255;
    static const uint8_t SWIFT_FILTER_BIT_NONE = 0;
    typedef std::vector<DocClusterMeta> DocClusterMetaVec;

public:
    ProcessedDocument() : _needSkip(false), _isUserDoc(true), _serializeFormat(SF_BINARY) {}
    ~ProcessedDocument() {}

public:
    std::shared_ptr<indexlibv2::document::IDocument> getDocument() const;
    std::shared_ptr<indexlib::document::Document> getLegacyDocument() const;

    void setDocumentBatch(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch);
    const std::shared_ptr<indexlibv2::document::IDocumentBatch>& getDocumentBatch() const;

    void setLocator(const common::Locator& locator);
    const common::Locator& getLocator() const;

    void setDocClusterMetaVec(const DocClusterMetaVec& vec);
    void addDocClusterMeta(const DocClusterMeta& meta);
    const DocClusterMetaVec& getDocClusterMetaVec() const;
    bool needSkip() const;
    void setNeedSkip(bool needSkip);
    void setTraceField(const std::string& traceField) { _traceField = traceField; }
    const std::string& getTraceField() { return _traceField; }
    void setIsUserDoc(bool isUserDoc);
    bool isUserDoc() const;

    void setDocSerializeFormat(SerializeFormat format);
    std::string transToDocString();

    const std::string& getRawDocString() const { return _rawDocStr; }
    void setRawDocString(const std::string& str) { _rawDocStr = str; }
    void enableSerializeRawDocument() { _serializeFormat = SF_RAW; }

    static std::string transToBinaryDocStr(const std::shared_ptr<indexlibv2::document::IDocument>& document)
    {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(document);
        return std::string(dataBuffer.getData(), dataBuffer.getDataLen());
    }

    static std::string transToRawDocStr(const RawDocumentPtr& rawDoc)
    {
        std::vector<autil::StringView> fieldNames;
        rawDoc->extractFieldNames(fieldNames);

        std::string rawDocStr;
        rawDocStr.reserve(1024);
        for (auto& field : fieldNames) {
            rawDocStr += std::string(field.data(), field.size());
            rawDocStr += "=";
            auto value = rawDoc->getField(field);
            rawDocStr += std::string(value.data(), value.size());
            rawDocStr += "\n";
        }

        if (rawDoc->GetTag("__print_document_tags__") == "true") {
            const auto& tagMap = rawDoc->GetTagInfoMap();
            for (const auto& [tagKey, tagValue] : tagMap) {
                rawDocStr += "__TAG__:" + tagKey;
                rawDocStr += "=";
                rawDocStr += tagValue;
                rawDocStr += "\n";
            }
        }
        rawDocStr += "\n";
        return rawDocStr;
    }

public:
    void TEST_setDocument(const std::shared_ptr<indexlibv2::document::IDocument>& document);

private:
    std::shared_ptr<indexlibv2::document::IDocumentBatch> _documentBatch;
    common::Locator _locator;
    DocClusterMetaVec _docClusterMeta;
    bool _needSkip;
    bool _isUserDoc;
    SerializeFormat _serializeFormat;
    std::string _traceField;
    std::string _rawDocStr;
};

BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);
/////////////////////////////////////////////////////

inline std::shared_ptr<indexlibv2::document::IDocument> ProcessedDocument::getDocument() const
{
    if (!_documentBatch) {
        return nullptr;
    }
    assert(_documentBatch->GetBatchSize() > 0);
    return (*_documentBatch)[0];
}

inline std::shared_ptr<indexlib::document::Document> ProcessedDocument::getLegacyDocument() const
{
    auto doc = getDocument();
    return std::dynamic_pointer_cast<indexlib::document::Document>(doc);
}

inline void ProcessedDocument::setDocumentBatch(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& docBatch)
{
    _documentBatch = docBatch;
}
inline const std::shared_ptr<indexlibv2::document::IDocumentBatch>& ProcessedDocument::getDocumentBatch() const
{
    return _documentBatch;
}

inline void ProcessedDocument::setLocator(const common::Locator& locator) { _locator = locator; }

inline const common::Locator& ProcessedDocument::getLocator() const { return _locator; }

inline void ProcessedDocument::setDocClusterMetaVec(const ProcessedDocument::DocClusterMetaVec& vec)
{
    _docClusterMeta = vec;
}

inline void ProcessedDocument::addDocClusterMeta(const DocClusterMeta& meta) { _docClusterMeta.push_back(meta); }

inline const ProcessedDocument::DocClusterMetaVec& ProcessedDocument::getDocClusterMetaVec() const
{
    return _docClusterMeta;
}

inline bool ProcessedDocument::needSkip() const { return _needSkip; }

inline void ProcessedDocument::setNeedSkip(bool needSkip) { _needSkip = needSkip; }

inline void ProcessedDocument::setIsUserDoc(bool isUserDoc) { _isUserDoc = isUserDoc; }

inline bool ProcessedDocument::isUserDoc() const { return _isUserDoc; }

inline std::string ProcessedDocument::transToDocString()
{
    std::shared_ptr<indexlibv2::document::IDocument> document = getDocument();
    assert(document);
    if (_serializeFormat == SF_RAW) {
        if (!_rawDocStr.empty()) {
            return _rawDocStr;
        }

        RawDocumentPtr rawDoc = DYNAMIC_POINTER_CAST(RawDocument, document);
        if (!rawDoc) {
            throw autil::legacy::ExceptionBase("document is not RawDocument type.");
        }
        _rawDocStr = transToRawDocStr(rawDoc);
        return _rawDocStr;
    }
    assert(_serializeFormat == SF_BINARY);
    const auto& kvDocumentBatch = std::dynamic_pointer_cast<indexlibv2::document::KVDocumentBatch>(_documentBatch);
    if (kvDocumentBatch) {
        autil::DataBuffer dataBuffer;
        dataBuffer.write(kvDocumentBatch);
        return std::string(dataBuffer.getData(), dataBuffer.getDataLen());
    }
    return transToBinaryDocStr(document);
}

inline void ProcessedDocument::TEST_setDocument(const std::shared_ptr<indexlibv2::document::IDocument>& document)
{
    if (!document) {
        _documentBatch.reset();
        return;
    }
    _documentBatch = std::make_shared<indexlibv2::document::DocumentBatch>();
    _documentBatch->AddDocument(document);
}

}} // namespace build_service::document

#endif // ISEARCH_BS_BS_PROCESSEDDOCUMENT_H
