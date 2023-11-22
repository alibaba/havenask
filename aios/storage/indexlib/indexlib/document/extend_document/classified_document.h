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

#include "indexlib/document/normal/ClassifiedDocument.h"

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::config {
class SourceSchema;
}

namespace indexlib::document {

using ClassifiedDocument = indexlibv2::document::ClassifiedDocument;
using ClassifiedDocumentPtr = std::shared_ptr<ClassifiedDocument>;
using IndexDocumentPtr = std::shared_ptr<IndexDocument>;

class SerializedSourceDocument;
class SourceDocument;

std::shared_ptr<SerializedSourceDocument>
createSerializedSourceDocument(const std::shared_ptr<SourceDocument>& sourceDocument,
                               const std::shared_ptr<config::SourceSchema>& sourceSchema, autil::mem_pool::Pool* pool);

} // namespace indexlib::document
