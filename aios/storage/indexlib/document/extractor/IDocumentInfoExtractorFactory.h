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

#include <any>
#include <memory>
#include <string>

#include "autil/NoCopyable.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/extractor/DocumentInfoExtractorType.h"

namespace indexlibv2::document::extractor {

class IDocumentInfoExtractor;
class IDocumentInfoExtractorFactory : public autil::NoCopyable
{
public:
    IDocumentInfoExtractorFactory() {}
    virtual ~IDocumentInfoExtractorFactory() {}

public:
    virtual std::unique_ptr<IDocumentInfoExtractor>
    CreateDocumentInfoExtractor(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                DocumentInfoExtractorType docExtractorType, std::any& fieldHint) = 0;
};

} // namespace indexlibv2::document::extractor
