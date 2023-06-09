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

#include "autil/NoCopyable.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcLeafIterator.h"
#include "orc/MemoryPool.hh"
#include "orc/Writer.hh"

namespace indexlib::file_system {
class FileWriter;
}

namespace indexlibv2::config {
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {

class InputStreamImpl;
class OutputStreamImpl;
class DocMapper;
class IOrcReader;

class OrcMerger : public autil::NoCopyable, public index::IIndexMerger
{
public:
    class ReadPlan
    {
    public:
        ReadPlan(segmentid_t segId, docid_t firstDocId, size_t sizeToRead)
            : _segId(segId)
            , _firstDocId(firstDocId)
            , _sizeToRead(sizeToRead)
        {
        }
        segmentid_t GetSegmentId() const { return _segId; }
        docid_t GetFirstDocId() const { return _firstDocId; }
        size_t GetSizeToRead() const { return _sizeToRead; }

    private:
        segmentid_t _segId;  // old seg id
        docid_t _firstDocId; // old local doc id, first doc to seek
        size_t _sizeToRead;  // doc count to read
    };

    OrcMerger() = default;
    ~OrcMerger() = default;

    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;
    orc::Writer* TEST_GetOrcWriter(segmentid_t targetSegId) const;

private:
    Status PrepareDocMapper(const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager,
                            std::shared_ptr<DocMapper>& docMapper);
    Status PrepareSrcSegmentIterators(const IIndexMerger::SegmentMergeInfos& segMergeInfos);
    Status PrepareSrcSegmentIterator(const std::shared_ptr<IOrcReader>& reader, segmentid_t srcSegmentId,
                                     docid_t baseDocId);
    Status PrepareTargetSegmentWriters(const IIndexMerger::SegmentMergeInfos& segMergeInfos);
    void PrepareTargetSegmentWriter(segmentid_t targetSegId,
                                    const std::shared_ptr<indexlib::file_system::FileWriter>& fileWriter,
                                    const std::shared_ptr<indexlibv2::config::OrcConfig>& orcConfig);

    bool GenerateReadPlans(size_t totalDocCount, docid_t globalNewDocId, const std::shared_ptr<DocMapper>& docMapper,
                           std::vector<std::shared_ptr<ReadPlan>>* plans) const;
    std::shared_ptr<ReadPlan> GenerateReadPlan(docid_t globalNewDocId, size_t leftDocCount,
                                               const std::shared_ptr<DocMapper>& docMapper) const;
    Status MergeOneSegment(const std::vector<std::shared_ptr<ReadPlan>>& readPlans, orc::Writer* writer) const;

    std::shared_ptr<config::OrcConfig> _config;
    std::vector<std::shared_ptr<OutputStreamImpl>> _outputStreams;
    std::shared_ptr<orc::MemoryPool> _pool;
    // src segment id to <baseDocId, leafIter>
    std::map<segmentid_t, std::pair<docid_t, std::shared_ptr<OrcLeafIterator>>> _srcSegIdToIterMap;
    // target segment id to writer
    std::map<segmentid_t, std::unique_ptr<orc::Writer>> _segIdToWriter;
    std::string _docMapperName;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
