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
#include "indexlib/document/kv/KVDocumentFactory.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IRawDocumentParser.h"
#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kv/KVDocumentParser.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KVDocumentFactory);

KVDocumentFactory::KVDocumentFactory() {}

KVDocumentFactory::~KVDocumentFactory() {}

std::unique_ptr<IDocumentParser>
KVDocumentFactory::CreateDocumentParser(const std::shared_ptr<config::TabletSchema>& schema,
                                        const std::shared_ptr<DocumentInitParam>& initParam)
{
    auto kvParser = std::make_unique<KVDocumentParser>();
    auto status = kvParser->Init(schema, initParam);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "kv document parser init failed, table[%s]", schema->GetTableName().c_str());
        return nullptr;
    }
    return kvParser;
}

} // namespace indexlibv2::document
