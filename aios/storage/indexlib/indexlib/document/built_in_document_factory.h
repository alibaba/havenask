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

#include "indexlib/common_define.h"
#include "indexlib/document/document_factory.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class BuiltInDocumentFactory : public DocumentFactory
{
public:
    static std::string MSG_TYPE;
    static std::string MSG_TYPE_DEFAULT;
    static std::string MSG_TYPE_FB;

public:
    BuiltInDocumentFactory();
    ~BuiltInDocumentFactory();

public:
    RawDocumentParser* CreateRawDocumentParser(const config::IndexPartitionSchemaPtr& schema,
                                               const DocumentInitParamPtr& initParam) override;

protected:
    RawDocument* CreateRawDocument(const config::IndexPartitionSchemaPtr& schema) override;

    RawDocument* CreateMultiMessageRawDocument(const config::IndexPartitionSchemaPtr& schema) override;

    DocumentParser* CreateDocumentParser(const config::IndexPartitionSchemaPtr& schema) override;

private:
    static const size_t MAX_KEY_SIZE;
    KeyMapManagerPtr mHashKeyMapManager;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuiltInDocumentFactory);
}} // namespace indexlib::document
