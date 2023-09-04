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
#include "indexlib/document/DocumentFactoryAdapter.h"

#include "autil/EnvUtil.h"
#include "indexlib/document/ExtendDocument.h"
#include "indexlib/document/IRawDocumentParser.h"
#include "indexlib/document/raw_document/DefaultRawDocument.h"
#include "indexlib/document/raw_document/KeyMapManager.h"

namespace indexlibv2::document {

DocumentFactoryAdapter::DocumentFactoryAdapter()
{
    size_t maxKeySize = autil::EnvUtil::getEnv("DEFAULT_RAWDOC_MAX_KEY_SIZE", MAX_KEY_SIZE);
    _hashKeyMapManager.reset(new KeyMapManager(maxKeySize));
}

DocumentFactoryAdapter::~DocumentFactoryAdapter() {}

std::unique_ptr<RawDocument>
DocumentFactoryAdapter::CreateRawDocument(const std::shared_ptr<config::ITabletSchema>& schema)
{
    return std::make_unique<DefaultRawDocument>(_hashKeyMapManager);
}

std::unique_ptr<ExtendDocument> DocumentFactoryAdapter::CreateExtendDocument()
{
    return std::make_unique<ExtendDocument>();
}

std::unique_ptr<IRawDocumentParser>
DocumentFactoryAdapter::CreateRawDocumentParser(const std::shared_ptr<config::ITabletSchema>& schema,
                                                const std::shared_ptr<DocumentInitParam>& initParam)
{
    return nullptr;
}

} // namespace indexlibv2::document
