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
#include "indexlib/index/field_meta/CommonSourceFieldMerger.h"

#include "indexlib/index/field_meta/Common.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, CommonSourceFieldMerger);

Status CommonSourceFieldMerger::Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                     const std::map<std::string, std::any>& params)
{
    auto iter = params.find(SOURCE_FIELD_READER_PARAM);
    if (iter == params.end()) {
        AUTIL_LOG(ERROR, "not find SOURCE_FIELD_READER_PARAM for index [%s]", indexConfig->GetIndexName().c_str());
        return Status::Corruption("not find SOURCE_FIELD_READER_PARAM");
    }
    try {
        _sourceFieldReaders = std::any_cast<std::map<segmentid_t, std::shared_ptr<ISourceFieldReader>>>(iter->second);
    } catch (const std::bad_any_cast& e) {
        AUTIL_LOG(ERROR, "not SOURCE_FIELD_READER_PARAM for index [%s]", indexConfig->GetIndexName().c_str());
        return Status::Corruption("not find SOURCE_FIELD_READER_PARAM");
    }
    return Status::OK();
}

std::pair<Status, std::shared_ptr<indexlibv2::index::IIndexer>>
CommonSourceFieldMerger::GetIndexer(const std::shared_ptr<indexlibv2::framework::Segment>& segment)
{
    auto iter = _sourceFieldReaders.find(segment->GetSegmentId());
    if (iter == _sourceFieldReaders.end()) {
        AUTIL_LOG(ERROR, "not find source field reader in segment [%d] for index [%s]", segment->GetSegmentId(),
                  _config->GetIndexName().c_str());
        return {Status::Corruption("not find source field reader"), nullptr};
    }
    auto sourceDiskIndexer = iter->second->GetSourceFieldDiskIndexer();
    if (!sourceDiskIndexer) {
        return {Status::Corruption("failed to get source field disk indexer"), nullptr};
    }
    return {Status::OK(), sourceDiskIndexer};
}

std::pair<Status, std::shared_ptr<file_system::IDirectory>>
CommonSourceFieldMerger::GetOutputDir(const std::shared_ptr<file_system::IDirectory>& segDir)
{
    return segDir->MakeDirectory(_config->GetIndexPath()[0], file_system::DirectoryOption()).StatusWith();
}

} // namespace indexlib::index
