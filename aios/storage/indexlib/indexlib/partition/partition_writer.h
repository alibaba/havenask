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
#ifndef __INDEXLIB_PARTITION_WRITER_H
#define __INDEXLIB_PARTITION_WRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Status.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);

namespace indexlib { namespace partition {

class PartitionWriter
{
public:
    // 描述从IndexPartitionReader或更上层视角感受到的Build模式：
    enum BuildMode {
        //// 流式。每调用一次 Build(Doc*) 就构建一次，单线程，返回后即可查询。
        BUILD_MODE_STREAM,

        //// 批式：攒批构建，多线程。内部达到攒批阈值或触发Dump条件后再实际构建，调用 Build(Doc*) 返回后不一定可查。

        // 一致性批式：对于每一个Doc，要么处于没有Build状态，要么处于已Build完成状态。
        // 上层对外提供查询服务时，应该使用该模式。
        // Build(Doc*) 通常返回很快，此时 doc 完全不可查。在触发攒批阈值时，会返回比较慢，此后该批及之前 doc
        // 所有字段均可查询。
        BUILD_MODE_CONSISTENT_BATCH,

        // 不一致性批式：对于一个Doc，可能部分字段已Build完成，而部分字段尚未Build。
        // 上层没有对外提供查询服务时，可以采用此模式，多线程Build的任务会跨批形成流水减少长尾，速度更快。
        // Build(Doc*) 返回很快，此时仅 pk 倒排可查，其它字段查询结果不确定。
        // 在触发 Dump 条件或切换 BuildMode 时需要等待对齐，此后该批及之前 doc 所有字段均可查。
        BUILD_MODE_INCONSISTENT_BATCH
    };
    static std::string BuildModeToString(PartitionWriter::BuildMode buildMode)
    {
        switch (buildMode) {
        case BUILD_MODE_STREAM:
            return "BUILD_MODE_STREAM";
        case BUILD_MODE_CONSISTENT_BATCH:
            return "BUILD_MODE_CONSISTENT_BATCH";
        case BUILD_MODE_INCONSISTENT_BATCH:
            return "BUILD_MODE_INCONSISTENT_BATCH";
        default:
            assert(false);
            return "BUILD_MODE_UNNOWN";
        }
    }

public:
    PartitionWriter(const config::IndexPartitionOptions& options, const std::string& partitionName = "")
        : mOptions(options)
        , mPartitionName(partitionName)
        , mLastConsumedMessageCount(0)
        , _buildMode(BuildMode::BUILD_MODE_STREAM)
    {
    }

    virtual ~PartitionWriter() {}

public:
    virtual void Open(const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionDataPtr& partitionData,
                      const PartitionModifierPtr& modifier) = 0;
    virtual void ReOpenNewSegment(const PartitionModifierPtr& modifier) = 0;
    virtual bool BuildDocument(const document::DocumentPtr& doc) = 0;
    virtual bool BatchBuild(std::unique_ptr<document::DocumentCollector> docCollector)
    {
        assert(false);
        return false;
    }
    virtual PartitionWriter::BuildMode GetBuildMode() { return _buildMode; }
    virtual void SwitchBuildMode(PartitionWriter::BuildMode buildMode) { _buildMode = buildMode; }
    virtual bool NeedDump(size_t memoryQuota, const document::DocumentCollector* docCollector = nullptr) const = 0;
    virtual void EndIndex() {};
    virtual void DumpSegment() = 0;
    virtual void Close() = 0;

    virtual void RewriteDocument(const document::DocumentPtr& doc) = 0;

    virtual uint64_t GetTotalMemoryUse() const = 0;
    virtual uint64_t GetBuildingSegmentDumpExpandSize() const
    {
        assert(false);
        return 0;
    }

    virtual void ReportMetrics() const = 0;
    virtual void SetEnableReleaseOperationAfterDump(bool releaseOperations) = 0;
    virtual bool NeedRewriteDocument(const document::DocumentPtr& doc);

    virtual util::Status GetStatus() const { return util::UNKNOWN; }
    virtual bool EnableAsyncDump() const { return false; }

public:
    size_t GetLastConsumedMessageCount() const { return mLastConsumedMessageCount; }

protected:
    config::IndexPartitionOptions mOptions;
    std::string mPartitionName;
    size_t mLastConsumedMessageCount;
    BuildMode _buildMode;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionWriter);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITION_WRITER_H
