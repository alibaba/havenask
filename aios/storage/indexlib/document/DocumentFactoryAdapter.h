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

#include "indexlib/document/IDocumentFactory.h"

namespace indexlibv2::document {
class KeyMapManager;

class DocumentFactoryAdapter : public IDocumentFactory
{
public:
    DocumentFactoryAdapter();
    ~DocumentFactoryAdapter();

public:
    std::unique_ptr<RawDocument> CreateRawDocument(const std::shared_ptr<config::ITabletSchema>& schema) override;

    std::unique_ptr<ExtendDocument> CreateExtendDocument() override;

    std::unique_ptr<IRawDocumentParser>
    CreateRawDocumentParser(const std::shared_ptr<config::ITabletSchema>& schema,
                            const std::shared_ptr<DocumentInitParam>& initParam) override;

private:
    std::shared_ptr<KeyMapManager> _hashKeyMapManager;
    static constexpr size_t MAX_KEY_SIZE = 4096;
};

} // namespace indexlibv2::document
