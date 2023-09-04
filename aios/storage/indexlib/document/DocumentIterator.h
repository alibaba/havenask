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
#include "autil/NoCopyable.h"
#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/IDocumentIterator.h"

namespace indexlibv2::document {
template <typename DocumentType>
class DocumentIterator : public IDocumentIterator
{
public:
    DocumentIterator(const TemplateDocumentBatch<DocumentType>* docBatch)
        : _docBatch(docBatch)
        , _typedDocBatch(docBatch)
        , _docIdx(-1)
    {
    }
    DocumentIterator(const IDocumentBatch* docBatch) : _docBatch(docBatch), _typedDocBatch(nullptr), _docIdx(-1) {}
    ~DocumentIterator() override {}

public:
    static std::unique_ptr<DocumentIterator<DocumentType>> Create(const TemplateDocumentBatch<DocumentType>* docBatch)
    {
        return std::make_unique<DocumentIterator<DocumentType>>(docBatch);
    }

    static std::unique_ptr<DocumentIterator<IDocument>> Create(const IDocumentBatch* docBatch)
    {
        return std::make_unique<DocumentIterator<IDocument>>(docBatch);
    }

public:
    // Callers should call HasNext() first and before calling other functions like Next() or GetDocIdx().
    // If HasNext() returns false, other functions are undefined.
    bool HasNext() const override;
    std::shared_ptr<IDocument> Next() override;
    std::shared_ptr<DocumentType> TypedNext();
    int64_t GetDocIdx() const override;

private:
    int64_t GetStep() const;

private:
    const IDocumentBatch* _docBatch;
    const TemplateDocumentBatch<DocumentType>* _typedDocBatch;
    int64_t _docIdx;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.document, DocumentIterator, DocumentType);

template <typename DocumentType>
int64_t DocumentIterator<DocumentType>::GetStep() const
{
    if (_docBatch == nullptr || _docBatch->GetBatchSize() == 0 || _docIdx + 1 >= _docBatch->GetBatchSize()) {
        return -1;
    }
    int64_t step = 1;
    while (_docIdx + step < _docBatch->GetBatchSize()) {
        if (!_docBatch->IsDropped(_docIdx + step)) {
            break;
        }
        ++step;
    }
    return step;
}

template <typename DocumentType>
bool DocumentIterator<DocumentType>::HasNext() const
{
    int64_t step = GetStep();
    return step > 0 && (_docIdx + step < _docBatch->GetBatchSize());
}

template <typename DocumentType>
std::shared_ptr<IDocument> DocumentIterator<DocumentType>::Next()
{
    _docIdx = _docIdx + GetStep();
    return (*_docBatch)[_docIdx];
}

template <typename DocumentType>
std::shared_ptr<DocumentType> DocumentIterator<DocumentType>::TypedNext()
{
    _docIdx = _docIdx + GetStep();
    return _typedDocBatch->GetTypedDocument(_docIdx);
}

template <typename DocumentType>
int64_t DocumentIterator<DocumentType>::GetDocIdx() const
{
    return _docIdx;
}

} // namespace indexlibv2::document
