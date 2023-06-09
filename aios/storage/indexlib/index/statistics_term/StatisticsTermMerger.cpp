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
#include "indexlib/index/statistics_term/StatisticsTermMerger.h"

#include "autil/StringTokenizer.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/index/statistics_term/Constant.h"
#include "indexlib/index/statistics_term/StatisticsTermDiskIndexer.h"
#include "indexlib/index/statistics_term/StatisticsTermFormatter.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/index/statistics_term/StatisticsTermLeafReader.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, StatisticsTermMerger);

Status StatisticsTermMerger::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const std::map<std::string, std::any>& params)
{
    _statConfig = std::dynamic_pointer_cast<StatisticsTermIndexConfig>(indexConfig);
    assert(_statConfig != nullptr);
    return Status::OK();
}

// seg1'/statistics_term/data
// seg2'/statistics_term/data   ->   seg0/statistics_term/data
// seg3'/statistics_term/data
Status StatisticsTermMerger::Merge(const SegmentMergeInfos& segMergeInfos,
                                   const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager)
{
    assert(segMergeInfos.targetSegments.size() > 0);
    auto segmentDir = segMergeInfos.targetSegments[0]->segmentDir->GetIDirectory();
    auto [st, indexDir] =
        segmentDir->MakeDirectory(index::STATISTICS_TERM_INDEX_PATH, indexlib::file_system::DirectoryOption())
            .StatusWith();
    RETURN_IF_STATUS_ERROR(st, "make index directory failed, index[%s/%s]", index::STATISTICS_TERM_INDEX_PATH.c_str(),
                           GetIndexName().c_str());
    auto [s, statDir] = indexDir->MakeDirectory(GetIndexName(), indexlib::file_system::DirectoryOption()).StatusWith();
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "make index directory %s failed, error: %s", GetIndexName().c_str(), s.ToString().c_str());
        return s;
    }

    std::shared_ptr<indexlib::file_system::FileWriter> dataFile;
    std::tie(s, dataFile) = CreateFileWriter(statDir, DATA_FILE);
    if (!s.IsOK()) {
        AUTIL_LOG(ERROR, "create data file failed, error: %s", s.ToString().c_str());
        return s;
    }

    std::vector<std::shared_ptr<StatisticsTermLeafReader>> statisticsTermLeafReaders;
    for (auto srcSegment : segMergeInfos.srcSegments) {
        auto segment = srcSegment.segment;
        auto [status, indexer] =
            segment->GetIndexer(STATISTICS_TERM_INDEX_TYPE_STR, _statConfig->GetIndexName().c_str());
        RETURN_IF_STATUS_ERROR(status, "get statistics term indexer failed, segId[%d]", segment->GetSegmentId());
        auto statIndexer = std::dynamic_pointer_cast<StatisticsTermDiskIndexer>(indexer);
        assert(statIndexer != nullptr);
        statisticsTermLeafReaders.emplace_back(statIndexer->GetReader());
    }

    size_t offset = 0;
    std::map<std::string, std::pair</*offset*/ size_t, /*blockLen*/ size_t>> termMetas;
    for (const auto& invertedIndexName : _statConfig->GetInvertedIndexNames()) {
        std::unordered_map<size_t, std::string> termStatistics;
        for (auto leafReader : statisticsTermLeafReaders) {
            auto status = leafReader->GetTermStatistics(invertedIndexName, termStatistics);
            RETURN_IF_STATUS_ERROR(status, "get statistics term failed, indexName[%s]", invertedIndexName.c_str());
        }
        std::string line = StatisticsTermFormatter::GetHeaderLine(invertedIndexName, termStatistics.size());
        size_t blockLen = line.size();
        RETURN_IF_STATUS_ERROR(dataFile->Write((void*)line.c_str(), line.length()).Status(),
                               "write statistics term info failed, line[%s]", line.c_str());
        for (const auto& [termHash, termStr] : termStatistics) {
            line = StatisticsTermFormatter::GetDataLine(termStr, termHash);
            RETURN_IF_STATUS_ERROR(dataFile->Write((void*)line.c_str(), line.length()).Status(),
                                   "write statistics term value failed, line[%s]", line.c_str());
            blockLen += line.size();
        }
        termMetas[invertedIndexName] = std::make_pair(offset, blockLen);
        offset += blockLen;
    }
    RETURN_IF_STATUS_ERROR(dataFile->Close().Status(), "close data file failed.");
    // write meta file
    std::string str = autil::legacy::ToJsonString(termMetas);
    s = statDir->Store(META_FILE, str, indexlib::file_system::WriterOption::AtomicDump()).Status();
    RETURN_IF_STATUS_ERROR(s, "write term meta failed");
    return Status::OK();
}

std::string StatisticsTermMerger::GetIndexName() const { return _statConfig->GetIndexName(); }

std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
StatisticsTermMerger::CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                       const std::string& fileName) const
{
    indexlib::file_system::WriterOption opts;
    opts.openType = indexlib::file_system::FSOT_BUFFERED;
    return directory->CreateFileWriter(fileName, opts).StatusWith();
}

} // namespace indexlibv2::index
