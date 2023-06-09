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
#include "indexlib/base/Types.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"
#include "indexlib/index/summary/Types.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace file_system {
class IDirectory;
}} // namespace indexlib::file_system

namespace indexlibv2 {
namespace framework {
struct SegmentMeta;
class Segment;
} // namespace framework
namespace config {
class SummaryIndexConfig;
class SummaryGroupConfig;
} // namespace config
} // namespace indexlibv2
namespace indexlibv2::index {
class DocMapper;
class SegmentMergeInfo;
class VarLenDataMerger;
class VarLenDataReader;
class VarLenDataWriter;
class LocalDiskSummaryDiskIndexer;

class LocalDiskSummaryMerger : private autil::NoCopyable
{
public:
    LocalDiskSummaryMerger(const std::shared_ptr<config::SummaryIndexConfig>& summaryGroupConfig,
                           summarygroupid_t groupId);
    ~LocalDiskSummaryMerger() = default;

public:
    Status Merge(const IIndexMerger::SegmentMergeInfos& segMergeInfos, const std::shared_ptr<DocMapper>& docMapper);

public:
    std::pair<Status, std::shared_ptr<VarLenDataMerger>>
    PrepareVarLenDataMerger(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                            const std::shared_ptr<DocMapper>& docMapper);
    std::pair<Status, std::shared_ptr<VarLenDataReader>>
    CreateSegmentReader(const IIndexMerger::SourceSegment& srcSegment);
    std::pair<Status, std::shared_ptr<VarLenDataWriter>>
    CreateSegmentWriter(const VarLenDataParam& param, const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta);

public:
    // mark the following function as virtual if the source indexer need ?
    VarLenDataParam CreateVarLenDataParam() const;
    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
    CreateOutputDirectory(const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta) const;

public:
    static const int DATA_BUFFER_SIZE = 1024 * 1024 * 2;

private:
    std::shared_ptr<config::SummaryIndexConfig> _summaryIndexConfig;
    std::shared_ptr<indexlibv2::config::SummaryGroupConfig> _summaryGroupConfig;
    summarygroupid_t _groupId;
    indexlib::util::SimplePool _pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
