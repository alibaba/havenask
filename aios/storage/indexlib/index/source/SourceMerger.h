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
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/index/IIndexMerger.h"
#include "indexlib/index/source/Types.h"
#include "indexlib/util/SimplePool.h"

namespace indexlibv2::config {
class SourceIndexConfig;
}

namespace indexlibv2 { namespace framework {
struct SegmentMeta;
class Segment;
}} // namespace indexlibv2::framework

namespace indexlibv2::index {

class DocMapper;
class SegmentMergeInfo;
class VarLenDataMerger;
class VarLenDataReader;
class VarLenDataWriter;
struct VarLenDataParam;

class SourceMerger : public IIndexMerger
{
public:
    SourceMerger() = default;
    ~SourceMerger() = default;

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::map<std::string, std::any>& params) override;
    Status Merge(const SegmentMergeInfos& segMergeInfos,
                 const std::shared_ptr<framework::IndexTaskResourceManager>& taskResourceManager) override;

private:
    std::string GetOutputPathFromSegment() const;
    index::sourcegroupid_t GetDataGroupId() const;
    bool IsMetaMerge() const;
    std::pair<Status, std::shared_ptr<VarLenDataMerger>>
    PrepareVarLenDataMerger(const IIndexMerger::SegmentMergeInfos& segMergeInfos,
                            const std::shared_ptr<DocMapper>& docMapper);
    std::pair<Status, std::shared_ptr<VarLenDataReader>>
    CreateSegmentReader(const VarLenDataParam& param, const IIndexMerger::SourceSegment& srcSegment);
    std::pair<Status, std::shared_ptr<VarLenDataWriter>>
    CreateSegmentWriter(const VarLenDataParam& param, const std::shared_ptr<framework::SegmentMeta>& outputSegmentMeta);

private:
    std::string _docMapperName;
    std::shared_ptr<config::SourceIndexConfig> _sourceConfig;
    indexlib::util::SimplePool _pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
