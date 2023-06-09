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
#ifndef __INDEXLIB_AITHETA_INDEX_SEGMENT_H
#define __INDEXLIB_AITHETA_INDEX_SEGMENT_H

#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"

namespace indexlib {
namespace aitheta_plugin {

class IndexSegment;
DEFINE_SHARED_PTR(IndexSegment);

class IndexSegment : public Segment {
 public:
    IndexSegment(const SegmentMeta& meta) : Segment(meta) {}
    ~IndexSegment() {}

 public:
    bool Load(const indexlib::file_system::DirectoryPtr& dir, const LoadType& type) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& dir) override;
    EmbedHolderIterator CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                  const std::set<catid_t>& validCatids) override;
    indexlib::file_system::FileWriterPtr
        CreateIndexWriter(const indexlib::file_system::DirectoryPtr& dir, bool deleteIfExisted = true);
    void SetDocIdMap(const DocIdMap &docIdMap) override;

 public:
    indexlib::file_system::FileWriterPtr GetIndexWriter() const { return mIndexWriter; }
    indexlib::file_system::FileReaderPtr GetIndexProvider() const { return mIndexProvider; }
    void AddOfflineIndexAttr(const OfflineIndexAttr& indexAttr) { mOfflineIndexAttrHolder.Add(indexAttr); }

    std::vector<IndexSegmentPtr> GetSubSegments() const { return mSubSegments; }

 private:
    bool InnerLoad(const indexlib::file_system::DirectoryPtr& dir, const LoadType& type);
    EmbedHolderIterator InnerCreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                       const std::set<catid_t>& validCatids);
    
 private:
    indexlib::file_system::FileReaderPtr mIndexProvider;
    indexlib::file_system::FileWriterPtr mIndexWriter;

    std::vector<IndexSegmentPtr> mSubSegments;
    IE_LOG_DECLARE();
};

}
}

#endif  //__INDEXLIB_AITHETA_INDEX_SEGMENT_H
