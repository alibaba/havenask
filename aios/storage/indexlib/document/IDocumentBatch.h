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

#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/Locator.h"

namespace indexlibv2::document {
class IDocument;
}

namespace indexlibv2::document {

class IDocumentBatch : public autil::NoCopyable
{
public:
    using DocumentPtr = std::shared_ptr<IDocument>;

public:
    IDocumentBatch() {}
    virtual ~IDocumentBatch() {}

    virtual std::unique_ptr<IDocumentBatch> Create() const = 0;
    virtual const framework::Locator& GetLastLocator() const = 0;
    virtual int64_t GetMaxTTL() const = 0;
    virtual void SetMaxTTL(int64_t maxTTL) = 0;
    virtual int64_t GetMaxTimestamp() const = 0;

    virtual void DropDoc(int64_t docIdx) = 0;
    virtual void ReleaseDoc(int64_t docIdx) = 0;
    virtual bool IsDropped(int64_t docIdx) const = 0;
    virtual size_t GetBatchSize() const = 0;
    virtual size_t GetValidDocCount() const = 0;
    virtual void AddDocument(DocumentPtr doc) = 0;
    virtual std::shared_ptr<IDocument> operator[](int64_t docIdx) const = 0;
    virtual size_t EstimateMemory() const = 0;
    virtual size_t GetAddedDocCount() const = 0;
};

} // namespace indexlibv2::document
