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
#ifndef __INDEXLIB_INDEX_MERGER_H
#define __INDEXLIB_INDEX_MERGER_H

#include <memory>
#include <sstream>

#include "indexlib/common_define.h"
#include "indexlib/config/merge_io_config.h"
#include "indexlib/index/normal/framework/merger_interface.h"
#include "indexlib/index_base/index_meta/merge_task_resource.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, TruncateIndexWriterCreator);

namespace indexlib { namespace index { namespace legacy {
class AdaptiveBitmapIndexWriterCreator;
class TruncateIndexWriterCreator;
}}} // namespace indexlib::index::legacy

namespace indexlib { namespace index {
class PostingFormat;

#define DECLARE_INDEX_MERGER_IDENTIFIER(id)                                                                            \
    static std::string Identifier() { return std::string("index.merger.") + std::string(#id); }                        \
    std::string GetIdentifier() const override { return Identifier(); }

class IndexMerger : public MergerInterface
{
public:
    IndexMerger();
    virtual ~IndexMerger();

public:
    virtual void Init(const config::IndexConfigPtr& indexConfig,
                      const index_base::MergeItemHint& hint = index_base::MergeItemHint(),
                      const index_base::MergeTaskResourceVector& taskResources = index_base::MergeTaskResourceVector(),
                      const config::MergeIOConfig& ioConfig = config::MergeIOConfig(),
                      legacy::TruncateIndexWriterCreator* truncateCreator = nullptr,
                      legacy::AdaptiveBitmapIndexWriterCreator* adaptiveCreator = nullptr);
    void BeginMerge(const SegmentDirectoryBasePtr& segDir) override;
    virtual std::string GetIdentifier() const = 0;

protected:
    config::IndexConfigPtr GetIndexConfig() const { return mIndexConfig; }
    const SegmentDirectoryBasePtr& GetSegmentDirectory() const { return mSegmentDirectory; }

    std::string GetMergedDir(const std::string& rootDir) const;

    file_system::DirectoryPtr GetMergeDir(const file_system::DirectoryPtr& indexDirectory, bool needClear) const;

protected:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::IndexConfigPtr mIndexConfig;
    config::MergeIOConfig mIOConfig;

    index_base::MergeItemHint mMergeHint;
    index_base::MergeTaskResourceVector mTaskResources;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMerger);
}} // namespace indexlib::index

#endif //__INDEXLIB_INDEX_MERGER_H
