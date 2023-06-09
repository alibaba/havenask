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
#include "indexlib/document/kv/KVDocumentBatch.h"

#include "KVDocumentBatch.h"
#include "autil/DataBuffer.h"
#include "autil/EnvUtil.h"
#include "indexlib/util/Exception.h"

namespace {
static const size_t poolChunkSize = []() {
    size_t chunkSize = 10 * 1024; // 10k
    chunkSize = autil::EnvUtil::getEnv("indexlib_kv_doc_pool_chunk", chunkSize);
    return chunkSize;
}();
}

namespace indexlibv2::document {

KVDocumentBatch::KVDocumentBatch() : _unsafePool(new autil::mem_pool::UnsafePool(poolChunkSize)) {}

KVDocumentBatch::~KVDocumentBatch() {}

std::unique_ptr<IDocumentBatch> KVDocumentBatch::Create() const { return std::make_unique<KVDocumentBatch>(); }

void KVDocumentBatch::AddDocument(DocumentPtr doc)
{
    auto kvDoc = std::dynamic_pointer_cast<KVDocument>(doc);
    assert(kvDoc);
    TemplateDocumentBatch<KVDocument>::AddDocument(kvDoc);
}

size_t KVDocumentBatch::GetAddedDocCount() const
{
    size_t validAddDoc = 0;
    for (size_t i = 0; i < GetBatchSize(); ++i) {
        auto opType = (*this)[i]->GetDocOperateType();
        if (!IsDropped(i) && (opType == ADD_DOC || opType == DELETE_DOC)) {
            validAddDoc++;
        }
    }
    return validAddDoc;
}

autil::mem_pool::UnsafePool* KVDocumentBatch::GetPool() const { return _unsafePool.get(); }

void KVDocumentBatch::serialize(autil::DataBuffer& dataBuffer) const
{
    uint32_t serializeVersion = KVDocument::MULTI_INDEX_KV_DOCUMENT_BINARY_VERSION;
    dataBuffer.write(serializeVersion);
    assert(serializeVersion >= KVDocument::KV_DOCUMENT_BINARY_VERSION);
    uint32_t docCount = GetBatchSize();
    dataBuffer.write(docCount);
    for (const auto& doc : _documents) {
        doc->serialize(dataBuffer);
    }

    if (serializeVersion >= KVDocument::MULTI_INDEX_KV_DOCUMENT_BINARY_VERSION) {
        for (const auto& doc : _documents) {
            doc->SerializeIndexNameHash(dataBuffer);
        }
    }
}

void KVDocumentBatch::deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t serializedVersion = 0;
    dataBuffer.read(serializedVersion);
    if (serializedVersion > KVDocument::LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD) {
        Clear();
        uint32_t docCount = 0;
        dataBuffer.read(docCount);
        for (size_t i = 0; i < docCount; ++i) {
            auto kvDoc = std::make_shared<KVDocument>(_unsafePool.get());
            kvDoc->deserialize(dataBuffer);
            TemplateDocumentBatch<KVDocument>::AddDocument(kvDoc);
        }

        if (serializedVersion >= KVDocument::MULTI_INDEX_KV_DOCUMENT_BINARY_VERSION) {
            for (auto& doc : _documents) {
                doc->DeserializeIndexNameHash(dataBuffer);
            }
        }
    } else {
        // TODO: support LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD serialize version ?
        INDEXLIB_THROW(indexlib::util::UnSupportedException,
                       "document deserialize failed, do not support deserialize kv document with version %u",
                       serializedVersion);
    }
}

} // namespace indexlibv2::document
