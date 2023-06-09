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
#include "indexlib/document/extend_document/classified_document.h"

#include "indexlib/config/source_schema.h"
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceDocument.h"

namespace indexlib::document {

std::shared_ptr<SerializedSourceDocument>
createSerializedSourceDocument(const std::shared_ptr<SourceDocument>& sourceDocument,
                               const std::shared_ptr<config::SourceSchema>& sourceSchema, autil::mem_pool::Pool* pool)
{
    if (!sourceDocument || !pool) {
        return nullptr;
    }
    SourceDocumentFormatter formatter;
    formatter.Init(sourceSchema);
    auto ret = std::make_shared<SerializedSourceDocument>();
    formatter.SerializeSourceDocument(sourceDocument, pool, ret);
    return ret;
}

} // namespace indexlib::document
