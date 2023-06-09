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
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"

namespace indexlibv2::plain {

class PrimaryKeyInfoExtractor : public indexlibv2::document::extractor::IDocumentInfoExtractor
{
public:
    PrimaryKeyInfoExtractor(const std::shared_ptr<config::IIndexConfig>& indexConfig);
    ~PrimaryKeyInfoExtractor();

public:
    Status ExtractField(document::IDocument* doc, void** field) override;
    bool IsValidDocument(document::IDocument* doc) override;

private:
    std::shared_ptr<config::IIndexConfig> _indexConfig;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
