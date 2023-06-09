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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_H
#define __INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_H

#include <tuple>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/misc/log.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/indexlib.h"

#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/segment.h"

namespace indexlib {
namespace aitheta_plugin {

class RawSegment : public Segment {
 public:
    RawSegment(const SegmentMeta& meta) : Segment(meta), mSize(0u), mMaxDocCount(0u) {}
    ~RawSegment() = default;

 public:
    bool Load(const indexlib::file_system::DirectoryPtr& dir, const LoadType& ty) override;
    bool Dump(const indexlib::file_system::DirectoryPtr& dir) override;
    EmbedHolderIterator CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                  const std::set<catid_t>& validCatids) override;

 public:
    size_t Size() const { return mSize; };
    size_t GetMaxDocCount() { return mMaxDocCount; };
    const std::set<catid_t> GetCatids() const;

 public:
    bool Add(catid_t categeory, docid_t docid, const embedding_t embedding);

 public:
    bool DumpEmbed(const indexlib::file_system::DirectoryPtr& dir);
    bool LoadEmbed(const indexlib::file_system::DirectoryPtr& dir);
    bool DumpMeta(const indexlib::file_system::DirectoryPtr& dir);

 private:
    indexlib::file_system::FileWriterPtr
        CreateEmbedWriter(const indexlib::file_system::DirectoryPtr& dir, bool deleteIfExisted = true) {
        if (deleteIfExisted) {
            indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
            dir->RemoveFile(EMBEDDING_FILE, removeOption);
        }
        return IndexlibIoWrapper::CreateWriter(dir, EMBEDDING_FILE);
    }

    indexlib::file_system::FileReaderPtr CreateEmbedProvider(const indexlib::file_system::DirectoryPtr& dir) {
        return IndexlibIoWrapper::CreateReader(dir, EMBEDDING_FILE);
    }

 private:
    std::map<catid_t, EmbedHolderPtr> mCatid2EmbedHolder;     // in
    indexlib::file_system::FileReaderPtr mEmbedProvider;  // out
    indexlib::file_system::FileReaderPtr mEmbedWriter;    // out
    size_t mSize;
    size_t mMaxDocCount;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RawSegment);

}
}

#endif  //__INDEXLIB_PLUGIN_PLUGINS_AITHETA_RAW_SEGMENT_H
