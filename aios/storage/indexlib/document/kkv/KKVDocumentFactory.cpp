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
#include "indexlib/document/kkv/KKVDocumentFactory.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IRawDocumentParser.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kkv/KKVDocumentParser.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KKVDocumentFactory);

KKVDocumentFactory::KKVDocumentFactory() {}

KKVDocumentFactory::~KKVDocumentFactory() {}

std::unique_ptr<document::IDocumentParser>
KKVDocumentFactory::CreateDocumentParser(const std::shared_ptr<config::TabletSchema>& schema,
                                         const std::shared_ptr<document::DocumentInitParam>& initParam)
{
    auto parser = std::make_unique<document::KKVDocumentParser>();
    auto status = parser->Init(schema, initParam);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "kkv document parser init failed, table[%s]", schema->GetTableName().c_str());
        return nullptr;
    }
    return parser;
}

} // namespace indexlibv2::document
