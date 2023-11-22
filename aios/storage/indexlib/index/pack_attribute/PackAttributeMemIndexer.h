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

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"
#include "indexlib/index/pack_attribute/PackAttributeIndexFields.h"

namespace indexlibv2::index {
class PackAttributeConfig;

class PackAttributeMemIndexer : public MultiValueAttributeMemIndexer<char>
{
public:
    PackAttributeMemIndexer(const MemIndexerParameter& indexerParam);
    ~PackAttributeMemIndexer();

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;

private:
    std::shared_ptr<PackAttributeConfig> _packAttributeConfig;
    bool _updatable = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
