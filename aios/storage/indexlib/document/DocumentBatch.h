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

#include "autil/Log.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
namespace indexlibv2::document {
template <typename DocumentType>
class TemplateDocumentBatch : public IDocumentBatch
{
public:
    using Self = TemplateDocumentBatch<DocumentType>;
    using DocumentPtrVec = std::vector<std::shared_ptr<DocumentType>>;
    using DocumentBitMap = std::vector<bool>;

public:
    TemplateDocumentBatch() : _droppedDocCount(0), _estimateMemorySize(0), _maxTimestamp(0), _maxTTL(0) {}

    ~TemplateDocumentBatch() override {}

public:
    std::unique_ptr<IDocumentBatch> Create() const override { return std::make_unique<Self>(); }
    const framework::Locator& GetLastLocator() const override;
    int64_t GetMaxTimestamp() const override { return _maxTimestamp; }
    int64_t GetMaxTTL() const override { return _maxTTL; }
    void SetMaxTTL(int64_t maxTTL) override { _maxTTL = maxTTL; }
    void DropDoc(int64_t docIdx) override;
    void ReleaseDoc(int64_t docIdx) override;
    size_t GetBatchSize() const override { return _documents.size(); }
    size_t GetValidDocCount() const override;
    void AddDocument(DocumentPtr doc) override;
    size_t EstimateMemory() const override { return _estimateMemorySize; };
    size_t GetAddedDocCount() const override;

public:
    // Try to avoid calling these methods, use IDocumentIterator instead.
    std::shared_ptr<IDocument> operator[](int64_t docIdx) const override;
    bool IsDropped(int64_t docIdx) const override { return _documentDroppedBitMap.at(docIdx); }

public:
    std::shared_ptr<IDocument> TEST_GetDocument(int64_t docIdx) const override { return operator[](docIdx); }

public:
    const std::shared_ptr<DocumentType>& GetTypedDocument(int64_t docIdx) const;
    void Clear();

protected:
    DocumentPtrVec _documents;
    DocumentBitMap _documentDroppedBitMap;

    size_t _droppedDocCount;
    size_t _estimateMemorySize;
    int64_t _maxTimestamp;
    int64_t _maxTTL;

private:
    template <typename T>
    friend class DocumentIterator;

protected:
    AUTIL_LOG_DECLARE();
};

template <typename DocumentType>
const framework::Locator& TemplateDocumentBatch<DocumentType>::GetLastLocator() const
{
    auto batchSize = GetBatchSize();
    if (!batchSize) {
        static framework::Locator invalidLocator;
        return invalidLocator;
    }
    return operator[](batchSize - 1)->GetLocatorV2();
}

template <typename DocumentType>
void TemplateDocumentBatch<DocumentType>::AddDocument(DocumentPtr doc)
{
    if (_maxTimestamp < doc->GetDocInfo().timestamp) {
        _maxTimestamp = doc->GetDocInfo().timestamp;
    }
    if (_maxTTL < doc->GetTTL()) {
        _maxTTL = doc->GetTTL();
    }
    _estimateMemorySize += doc->EstimateMemory();
    _documents.push_back(std::dynamic_pointer_cast<DocumentType>(doc));
    _documentDroppedBitMap.push_back(false);
}

template <typename DocumentType>
size_t TemplateDocumentBatch<DocumentType>::GetAddedDocCount() const
{
    size_t validAddDoc = 0;
    for (size_t i = 0; i < GetBatchSize(); ++i) {
        auto opType = (*this)[i]->GetDocOperateType();
        if (!IsDropped(i) && (opType == ADD_DOC)) {
            validAddDoc++;
        }
    }
    return validAddDoc;
}

template <typename DocumentType>
void TemplateDocumentBatch<DocumentType>::Clear()
{
    _documents.clear();
    _documentDroppedBitMap.clear();
    _droppedDocCount = 0;
    _estimateMemorySize = 0;
    _maxTimestamp = 0;
    _maxTTL = 0;
}

template <typename DocumentType>
std::shared_ptr<IDocument> TemplateDocumentBatch<DocumentType>::operator[](int64_t docIdx) const
{
    if (docIdx >= _documents.size()) {
        return nullptr;
    }

    return _documents[docIdx];
}

template <typename DocumentType>
const std::shared_ptr<DocumentType>& TemplateDocumentBatch<DocumentType>::GetTypedDocument(int64_t docIdx) const
{
    if (docIdx >= _documents.size()) {
        static std::shared_ptr<DocumentType> empty;
        return empty;
    }

    return _documents[docIdx];
}

template <typename DocumentType>
void TemplateDocumentBatch<DocumentType>::DropDoc(int64_t docIdx)
{
    if (docIdx >= _documentDroppedBitMap.size()) {
        AUTIL_LOG(ERROR, "can not locate doc in drop operation.");
        return;
    }
    if (!_documentDroppedBitMap.at(docIdx)) {
        _documentDroppedBitMap[docIdx] = true;
        _estimateMemorySize -= _documents[docIdx]->EstimateMemory();
        _droppedDocCount++;
    }
}

template <typename DocumentType>
void TemplateDocumentBatch<DocumentType>::ReleaseDoc(int64_t docIdx)
{
    if (docIdx < 0 || docIdx >= _documents.size()) {
        AUTIL_LOG(ERROR, "can not locate doc in release operation.");
        return;
    }
    _documents[docIdx].reset();
}

template <typename DocumentType>
size_t TemplateDocumentBatch<DocumentType>::GetValidDocCount() const
{
    return _documentDroppedBitMap.size() - _droppedDocCount;
}

AUTIL_LOG_SETUP_TEMPLATE(indexlib.document, TemplateDocumentBatch, DocumentType);

using DocumentBatch = TemplateDocumentBatch<IDocument>;

} // namespace indexlibv2::document
