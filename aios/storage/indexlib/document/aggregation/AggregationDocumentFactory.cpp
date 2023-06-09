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
#include "indexlib/document/aggregation/AggregationDocumentFactory.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/document/PlainDocumentParser.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, AggregationDocumentFactory);

AggregationDocumentFactory::AggregationDocumentFactory() {}

AggregationDocumentFactory::~AggregationDocumentFactory() {}

std::unique_ptr<IDocumentParser>
AggregationDocumentFactory::CreateDocumentParser(const std::shared_ptr<config::TabletSchema>& schema,
                                                 const std::shared_ptr<DocumentInitParam>& initParam)
{
    auto parser = std::make_unique<PlainDocumentParser>();
    auto status = parser->Init(schema, initParam);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "aggregation document parser init failed, table[%s]", schema->GetTableName().c_str());
        return nullptr;
    }
    return parser;
}

} // namespace indexlibv2::document
