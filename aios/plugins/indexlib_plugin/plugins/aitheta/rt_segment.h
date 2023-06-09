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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RT_SEGMENT_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RT_SEGMENT_H

#include <map>
#include <string>
#include <vector>
#include <aitheta/index_factory.h>
#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"
#include "autil/Lock.h"
#include "indexlib_plugin/plugins/aitheta/util/search_util.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/index/rt_index.h"

namespace indexlib {
namespace aitheta_plugin {

class RtSegment : public Segment {
 public:
    RtSegment(const SegmentMeta& meta, const SchemaParameter& schemaParameter)
        : Segment(meta), mSchemaParam(schemaParameter), mDocCount(0u) {}
    ~RtSegment() = default;

 public:
    bool IsAvailable() const {
        autil::ScopedReadLock rdLock(mHolderLock);
        return mOnlineIndexHolder != nullptr;
    }
    // call IsAvailable() first
    bool Add(int64_t categeory, docid_t docid, const EmbeddingPtr embedding);

 public:
    bool Load(const indexlib::file_system::DirectoryPtr& dir, const LoadType& ty) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& dir) override;
    EmbedHolderIterator CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                  const std::set<catid_t>& validCatids) override;

 public:
    OnlineIndexHolderPtr& GetOnlineIndexHolder() {
        autil::ScopedReadLock rdLock(mHolderLock);
        return mOnlineIndexHolder;
    }

    void SetOnlineIndexHolder(const OnlineIndexHolderPtr& holder) {
        autil::ScopedWriteLock wrLock(mHolderLock);
        if (mOnlineIndexHolder) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "mOnlineIndexHolder is already set");
        }
        mOnlineIndexHolder = holder;
    }

 public:
    // TODO mutil category latency
    size_t Size();

 private:
    bool DumpIndex(catid_t catid, const OnlineIndexPtr& indexAttr);
    bool DumpMeta(const indexlib::file_system::DirectoryPtr& dir);

 private:
    SchemaParameter mSchemaParam;
    // only protect mOnlineIndexHolder, but not the member in it
    mutable autil::ReadWriteLock mHolderLock;
    OnlineIndexHolderPtr mOnlineIndexHolder;
    size_t mDocCount;
    indexlib::file_system::FileWriterPtr mIndexWriter;

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RtSegment);

}
}

#endif  //  __INDEXLIB_PLUGIN_PLUGINS_AITHETA_IN_MEMORY_SEGMENT_H
