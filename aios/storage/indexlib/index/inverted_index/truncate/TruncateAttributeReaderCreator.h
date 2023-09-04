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
#include <mutex>
#include <string>

#include "autil/Log.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::index {
class DocMapper;
class AttributeDiskIndexer;
} // namespace indexlibv2::index

namespace indexlib::index {
class TruncateAttributeReader;

class TruncateAttributeReaderCreator
{
public:
    TruncateAttributeReaderCreator(const std::shared_ptr<indexlibv2::config::ITabletSchema>& tabletSchema,
                                   const indexlibv2::index::IIndexMerger::SegmentMergeInfos& segmentMergeInfos,
                                   const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper);
    ~TruncateAttributeReaderCreator() = default;

public:
    std::shared_ptr<TruncateAttributeReader> Create(const std::string& attrName);
    const std::shared_ptr<indexlibv2::config::ITabletSchema>& GetTabletSchema() const { return _tabletSchema; }
    const std::shared_ptr<indexlibv2::index::DocMapper>& GetDocMapper() const { return _docMapper; }
    std::shared_ptr<indexlibv2::index::AttributeConfig> GetAttributeConfig(const std::string& fieldName) const;

private:
    std::shared_ptr<TruncateAttributeReader> CreateAttributeReader(const std::string& fieldName);

private:
    mutable std::mutex _mutex;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    std::shared_ptr<indexlibv2::index::DocMapper> _docMapper;
    indexlibv2::index::IIndexMerger::SegmentMergeInfos _segmentMergeInfos;
    std::map<std::string, std::shared_ptr<TruncateAttributeReader>> _truncateAttributeReaders;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
