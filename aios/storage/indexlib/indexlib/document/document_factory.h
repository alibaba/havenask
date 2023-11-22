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
#include "indexlib/document/document_parser.h"
#include "indexlib/document/raw_document.h"
#include "indexlib/document/raw_document_parser.h"
#include "indexlib/indexlib.h"
#include "indexlib/plugin/ModuleFactory.h"

namespace indexlib { namespace document {

class DocumentFactory : public plugin::ModuleFactory
{
public:
    DocumentFactory();
    virtual ~DocumentFactory();

public:
    void destroy() override { delete this; }

public:
    /* create raw document */
    RawDocument* CreateRawDocument(const config::IndexPartitionSchemaPtr& schema,
                                   const DocumentInitParamPtr& initParam);

    /* create multi message raw document */
    RawDocument* CreateMultiMessageRawDocument(const config::IndexPartitionSchemaPtr& schema,
                                               const DocumentInitParamPtr& initParam);

    /* create document parser */
    DocumentParser* CreateDocumentParser(const config::IndexPartitionSchemaPtr& schema,
                                         const DocumentInitParamPtr& initParam);

    /* create raw document parser */
    virtual RawDocumentParser* CreateRawDocumentParser(const config::IndexPartitionSchemaPtr& schema,
                                                       const DocumentInitParamPtr& initParam)
    {
        return nullptr;
    }

protected:
    virtual RawDocument* CreateRawDocument(const config::IndexPartitionSchemaPtr& schema) = 0;

    virtual DocumentParser* CreateDocumentParser(const config::IndexPartitionSchemaPtr& schema) = 0;

    virtual RawDocument* CreateMultiMessageRawDocument(const config::IndexPartitionSchemaPtr& schema)
    {
        return nullptr;
    }

public:
    static const std::string DOCUMENT_FACTORY_NAME;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocumentFactory);

// user must provider this two functions:
//     1. create${DOCUMENT_FACTORY_NAME}
//     2. destory${DOCUMENT_FACTORY_NAME}
extern "C" plugin::ModuleFactory* createDocumentFactory();
extern "C" void destroyDocumentFactory(plugin::ModuleFactory* factory);
}} // namespace indexlib::document
