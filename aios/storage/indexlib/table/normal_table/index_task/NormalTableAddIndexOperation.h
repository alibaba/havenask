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
#include "autil/NoCopyable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/IDocumentParser.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/index_task/IndexOperation.h"
#include "indexlib/framework/index_task/IndexOperationDescription.h"
#include "indexlib/index/IMemIndexer.h"

namespace indexlibv2::table {

class NormalTableAddIndexOperation : public framework::IndexOperation
{
public:
    NormalTableAddIndexOperation(const framework::IndexOperationDescription& opDesc);
    ~NormalTableAddIndexOperation();

public:
    static constexpr char OPERATION_TYPE[] = "normal_table_add_index";
    static constexpr char TARGET_SCHEMA_ID[] = "target_schema_id";
    static constexpr char TARGET_SEGMENT_ID[] = "target_segment_id";
    static constexpr char TARGET_INDEX_NAME[] = "target_index_name";
    static constexpr char TARGET_INDEX_TYPE[] = "target_index_type";

    Status Execute(const framework::IndexTaskContext& context) override;
    static framework::IndexOperationDescription
    CreateOperationDescription(framework::IndexOperationId id, schemaid_t targetSchemaId, segmentid_t segId,
                               std::shared_ptr<config::IIndexConfig> indexConfig);

private:
    Status PrepareMemIndexer(const framework::IndexTaskContext& context);
    Status PrepareDocIterAndParser(const framework::IndexTaskContext& context);

private:
    framework::IndexOperationDescription _opDesc;
    schemaid_t _targetSchemaId = INVALID_SCHEMAID;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::shared_ptr<config::IIndexConfig> _indexConfig;
    std::shared_ptr<framework::TabletData> _newTabletData;
    std::shared_ptr<framework::ITabletDocIterator> _docIter;
    std::shared_ptr<document::IDocumentParser> _docParser;
    std::shared_ptr<index::IMemIndexer> _indexer;
    segmentid_t _targetSegmentId = INVALID_SEGMENTID;
    size_t _targetSegmentDocCount = 0;
    std::string _segmentDir;
    std::string _indexDir;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
