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

#include "autil/Log.h"
#include "indexlib/table/common/CommonTabletWriter.h"

namespace indexlibv2::index {
class AttributeReference;
class KVIndexReader;
class PackAttributeFormatter;
} // namespace indexlibv2::index

namespace indexlibv2::table {

class AggregationTabletReader;
class DeleteGenerator;

class AggregationTabletWriter final : public CommonTabletWriter
{
public:
    AggregationTabletWriter(const std::shared_ptr<config::TabletSchema>& schema, const config::TabletOptions* options);
    ~AggregationTabletWriter();

public:
    Status DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                  const framework::BuildResource& buildResource, const framework::OpenOptions& openOptions) override;
    Status DoBuild(const std::shared_ptr<document::IDocumentBatch>& batch) override;

private:
    bool IsNewerVersion(const autil::StringView& currentVersion, const autil::StringView& lastVersion) const;

private:
    std::shared_ptr<AggregationTabletReader> _tabletReader;
    std::shared_ptr<index::KVIndexReader> _primaryKeyReader;
    std::unique_ptr<DeleteGenerator> _deleteGenerator;
    std::string _pkIndexType;
    uint64_t _pkIndexNameHash = 0;
    std::shared_ptr<index::PackAttributeFormatter> _packAttrFormatter;
    const index::AttributeReference* _versionRef = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
