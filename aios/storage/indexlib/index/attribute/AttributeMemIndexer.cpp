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
#include "indexlib/index/attribute/AttributeMemIndexer.h"

#include "indexlib/document/IDocument.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributeMemIndexer);

Status AttributeMemIndexer::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                 document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory)
{
    _allocator.reset(new indexlib::util::MMapAllocator);
    _pool.reset(new autil::mem_pool::Pool(_allocator.get(), DEFAULT_CHUNK_SIZE * 1024 * 1024));
    _attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(_attrConfig);
    _attrConvertor =
        std::shared_ptr<AttributeConvertor>(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(_attrConfig));

    auto fieldId = std::make_any<fieldid_t>(_attrConfig->GetFieldId());
    assert(docInfoExtractorFactory);
    _docInfoExtractor = docInfoExtractorFactory->CreateDocumentInfoExtractor(
        indexConfig, document::extractor::DocumentInfoExtractorType::ATTRIBUTE_FIELD, fieldId);

    return Status::OK();
}

Status AttributeMemIndexer::AddDocument(document::IDocument* doc)
{
    auto docId = doc->GetDocId();
    if (doc->GetDocOperateType() != ADD_DOC) {
        AUTIL_LOG(DEBUG, "doc[%d] isn't add_doc", docId);
        return Status::OK();
    }
    std::pair<autil::StringView, bool> fieldPair;
    auto pointer = &fieldPair;
    RETURN_STATUS_DIRECTLY_IF_ERROR(_docInfoExtractor->ExtractField(doc, (void**)(&pointer)));
    AddField(docId, fieldPair.first, fieldPair.second);
    return Status::OK();
}
Status AttributeMemIndexer::Build(const document::IIndexFields* indexFields, size_t n)
{
    assert(0);
    return {};
}
Status AttributeMemIndexer::Build(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (!docBatch->IsDropped(i)) {
            std::shared_ptr<document::IDocument> doc = (*docBatch)[i];
            RETURN_STATUS_DIRECTLY_IF_ERROR(AddDocument(doc.get()));
        }
    }
    return Status::OK();
}

bool AttributeMemIndexer::IsValidDocument(document::IDocument* doc) { return _docInfoExtractor->IsValidDocument(doc); }
bool AttributeMemIndexer::IsValidField(const document::IIndexFields* fields)
{
    assert(0);
    return false;
}

void AttributeMemIndexer::ValidateDocumentBatch(document::IDocumentBatch* docBatch)
{
    for (size_t i = 0; i < docBatch->GetBatchSize(); ++i) {
        if (!docBatch->IsDropped(i)) {
            if (!_docInfoExtractor->IsValidDocument((*docBatch)[i].get())) {
                docBatch->DropDoc(i);
            }
        }
    }
}

} // namespace indexlibv2::index
