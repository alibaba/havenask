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
#include "indexlib/document/document_factory.h"

#include "indexlib/document/raw_document.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DocumentFactory);

const string DocumentFactory::DOCUMENT_FACTORY_NAME = "DocumentFactory";

DocumentFactory::DocumentFactory() {}

DocumentFactory::~DocumentFactory() {}

RawDocument* DocumentFactory::CreateRawDocument(const IndexPartitionSchemaPtr& schema,
                                                const DocumentInitParamPtr& initParam)
{
    unique_ptr<RawDocument> rawDoc(CreateRawDocument(schema));
    if (!rawDoc) {
        ERROR_COLLECTOR_LOG(ERROR, "create raw document fail!");
        return nullptr;
    }
    if (!rawDoc->Init(initParam)) {
        ERROR_COLLECTOR_LOG(ERROR, "init raw document fail!");
        return nullptr;
    }
    return rawDoc.release();
}

RawDocument* DocumentFactory::CreateMultiMessageRawDocument(const IndexPartitionSchemaPtr& schema,
                                                            const DocumentInitParamPtr& initParam)
{
    unique_ptr<RawDocument> rawDoc(CreateMultiMessageRawDocument(schema));
    if (!rawDoc) {
        ERROR_COLLECTOR_LOG(ERROR, "create multi message raw document fail!");
        return nullptr;
    }
    if (!rawDoc->Init(initParam)) {
        ERROR_COLLECTOR_LOG(ERROR, "init multi message raw document fail!");
        return nullptr;
    }
    return rawDoc.release();
}

DocumentParser* DocumentFactory::CreateDocumentParser(const IndexPartitionSchemaPtr& schema,
                                                      const DocumentInitParamPtr& initParam)
{
    unique_ptr<DocumentParser> parser(CreateDocumentParser(schema));
    if (!parser) {
        ERROR_COLLECTOR_LOG(ERROR, "create document parser fail!");
        return nullptr;
    }

    if (!parser->Init(initParam)) {
        ERROR_COLLECTOR_LOG(ERROR, "init DocumentParser fail!");
        return nullptr;
    }
    return parser.release();
}
}} // namespace indexlib::document
