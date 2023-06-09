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
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/attribute/AttributeFactory.h"
#include "indexlib/index/attribute/Common.h"
namespace indexlibv2::index {

AUTIL_LOG_SETUP(indexlib.index, AttributeDefaultDiskIndexerFactory);

std::pair<Status, std::shared_ptr<IDiskIndexer>>
AttributeDefaultDiskIndexerFactory::CreateDefaultDiskIndexer(const std::shared_ptr<framework::Segment>& segment,
                                                             const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    index::IndexerParameter indexerParam;
    indexerParam.segmentId = segment->GetSegmentId();
    indexerParam.docCount = segment->GetSegmentInfo()->docCount;
    indexerParam.segmentInfo = segment->GetSegmentInfo();
    indexerParam.segmentMetrics = segment->GetSegmentMetrics();
    indexerParam.readerOpenType = index::IndexerParameter::READER_DEFAULT_VALUE;

    AttributeDiskIndexerCreator* creator =
        AttributeFactory<AttributeDiskIndexer, AttributeDiskIndexerCreator>::GetInstance()->GetAttributeInstanceCreator(
            indexConfig);
    assert(creator);
    std::shared_ptr<IDiskIndexer> diskIndexer = creator->Create(nullptr, indexerParam);
    auto [status, directory] =
        segment->GetSegmentDirectory()->GetIDirectory()->GetDirectory(ATTRIBUTE_INDEX_PATH).StatusWith();
    RETURN2_IF_STATUS_ERROR(status, nullptr, "get attribute path failed, segment [%d]", segment->GetSegmentId());

    status = diskIndexer->Open(indexConfig, directory);
    RETURN2_IF_STATUS_ERROR(status, nullptr, "open attribute [%s] default value indexer for segment [%d] failed",
                            indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
    return {Status::OK(), diskIndexer};
}

} // namespace indexlibv2::index
