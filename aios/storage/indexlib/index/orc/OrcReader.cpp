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
#include "indexlib/index/orc/OrcReader.h"

#include "autil/Log.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcDiskIndexer.h"
#include "indexlib/index/orc/OrcIterator.h"
#include "indexlib/index/orc/TypeUtils.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(index, OrcReader);

OrcReader::OrcReader() {}

OrcReader::OrcReader(std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders)
    : _segmentReaders(std::move(segmentReaders))
{
}

OrcReader::OrcReader(std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders,
                     const std::shared_ptr<config::OrcConfig>& config)
    : _segmentReaders(std::move(segmentReaders))
    , _config(config)
{
}

Status OrcReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                       const framework::TabletData* tabletData)
{
    std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders;
    auto segments = tabletData->CreateSlice();
    for (auto it = segments.rbegin(); it != segments.rend(); it++) {
        const auto& segment = *it;

        auto indexerPair = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            return Status::InternalError("no indexer for %s in segment %d", indexConfig->GetIndexName().c_str(),
                                         segment->GetSegmentId());
        }
        auto indexer = indexerPair.second;
        auto status = segment->GetSegmentStatus();
        std::shared_ptr<IOrcReader> reader;
        if (status == framework::Segment::SegmentStatus::ST_BUILT) {
            OrcDiskIndexer* diskIndexer = dynamic_cast<OrcDiskIndexer*>(indexer.get());
            if (diskIndexer != nullptr) {
                reader = diskIndexer->GetReader();
            }
        } else {
            // not yet support read orc mem indexer
            continue;
        }
        if (!reader) {
            return Status::InternalError("failed to create reader for segment: %d", segment->GetSegmentId());
        }
        segmentReaders.emplace(segment->GetSegmentId(), reader);
    }

    _config = std::dynamic_pointer_cast<config::OrcConfig>(indexConfig);

    if (_config == nullptr) {
        return Status::InternalError("create orc config failed");
    }

    _segmentReaders = std::move(segmentReaders);
    return Status::OK();
}

Status OrcReader::CreateIterator(const ReaderOptions& opts, std::unique_ptr<IOrcIterator>& orcIter)
{
    std::vector<std::shared_ptr<IOrcReader>> selectedSegmentReaders;
    for (auto& it : _segmentReaders) {
        selectedSegmentReaders.emplace_back(it.second);
    }
    if (_config == nullptr) {
        return Status::InternalError("orc reader need config");
    }
    auto type = TypeUtils::MakeOrcTypeFromConfig(_config.get(), opts.fieldIds);
    orcIter = std::make_unique<OrcIterator>(selectedSegmentReaders, opts, std::move(type));
    return Status::OK();
}

uint64_t OrcReader::GetTotalRowCount() const
{
    uint64_t totalRowCount = 0;
    for (const auto& segmentReader : _segmentReaders) {
        totalRowCount += segmentReader.second->GetTotalRowCount();
    }
    return totalRowCount;
}

size_t OrcReader::EvaluateCurrentMemUsed() const
{
    size_t totalSize = 0;
    for (const auto& iter : _segmentReaders) {
        totalSize += iter.second->EvaluateCurrentMemUsed();
    }
    return totalSize;
}

std::unique_ptr<OrcReader> OrcReader::Slice(const std::vector<segmentid_t>& segmentIds)
{
    if (segmentIds.size() == 0) {
        return std::make_unique<OrcReader>(_segmentReaders, _config);
    } else {
        std::map<segmentid_t, std::shared_ptr<IOrcReader>> segmentReaders;
        for (const auto& segmentId : segmentIds) {
            const auto& iter = _segmentReaders.find(segmentId);
            if (iter == _segmentReaders.end()) {
                AUTIL_LOG(ERROR, "not found segment id %u", segmentId);
                return nullptr;
            }
            segmentReaders.emplace(std::make_pair(segmentId, iter->second));
        }
        return std::make_unique<OrcReader>(segmentReaders, _config);
    }
}

const std::shared_ptr<config::OrcConfig>& OrcReader::GetConfig() const { return _config; }

} // namespace indexlibv2::index
