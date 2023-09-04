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

#include "indexlib/base/Status.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlib::document {
class ModifiedTokens;
class IndexDocument;
} // namespace indexlib::document
namespace indexlibv2::document::extractor {
class IDocumentInfoExtractor;
} // namespace indexlibv2::document::extractor

namespace indexlibv2::index {
class AttributeMemReader;
} // namespace indexlibv2::index

namespace indexlib::index {
class InvertedIndexMetrics;

class IInvertedMemIndexer : public indexlibv2::index::IMemIndexer
{
public:
    IInvertedMemIndexer() = default;
    virtual ~IInvertedMemIndexer() = default;

    virtual void UpdateTokens(docid_t docId, const document::ModifiedTokens& modifiedTokens) = 0;
    virtual std::shared_ptr<InvertedIndexMetrics> GetMetrics() const = 0;
    virtual std::shared_ptr<indexlibv2::index::AttributeMemReader> CreateSectionAttributeMemReader() const = 0;

public:
    virtual indexlibv2::document::extractor::IDocumentInfoExtractor* GetDocInfoExtractor() const = 0;

private:
    friend class SingleInvertedIndexBuilder;
};

} // namespace indexlib::index
