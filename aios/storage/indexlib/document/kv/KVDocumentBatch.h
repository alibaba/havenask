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

#include "indexlib/document/DocumentBatch.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/kv/KVDocument.h"

namespace autil::mem_pool {
class UnsafePool;
}

namespace autil {
class DataBuffer;
}

namespace indexlibv2::document {

class KVDocumentBatch : public TemplateDocumentBatch<KVDocument>
{
public:
    KVDocumentBatch();
    ~KVDocumentBatch() override;

    std::unique_ptr<IDocumentBatch> Create() const override;

    void AddDocument(DocumentPtr doc) override;
    size_t GetAddedDocCount() const override;
    autil::mem_pool::UnsafePool* GetPool() const;

    virtual void serialize(autil::DataBuffer& dataBuffer) const;
    virtual void deserialize(autil::DataBuffer& dataBuffer);

private:
    std::unique_ptr<autil::mem_pool::UnsafePool> _unsafePool;
};

} // namespace indexlibv2::document
