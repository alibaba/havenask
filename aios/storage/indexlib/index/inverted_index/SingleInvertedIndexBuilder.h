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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/common/IndexerOrganizerMeta.h"
#include "indexlib/index/inverted_index/InvertedIndexerOrganizerUtil.h"

namespace indexlib::index {
class SingleInvertedIndexBuilder : public autil::NoCopyable
{
public:
    SingleInvertedIndexBuilder();
    ~SingleInvertedIndexBuilder();

public:
    Status AddDocument(indexlibv2::document::IDocument* doc);
    Status UpdateDocument(indexlibv2::document::IDocument* doc);

public:
    std::string GetIndexName() const;

public:
    Status Init(const indexlibv2::framework::TabletData& tabletData,
                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& outerIndexConfig,
                const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& innerIndexConfig, size_t shardId,
                bool isOnline);

private:
    bool _shouldSkipUpdateIndex = false;
    IndexerOrganizerMeta _indexerOrganizerMeta;
    SingleInvertedIndexBuildInfoHolder _buildInfoHolder;

    std::unique_ptr<indexlibv2::document::extractor::IDocumentInfoExtractor> _docInfoExtractor;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
