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
#ifndef __INDEXLIB_AITHETA_SEGMENT_H
#define __INDEXLIB_AITHETA_SEGMENT_H

#include <map>
#include <string>
#include <vector>

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/inverted_index/customized_index/indexer_define.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/segment_meta.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/embed_holder_iterator.h"
#include "indexlib_plugin/plugins/aitheta/util/knn_config.h"
#include "indexlib_plugin/plugins/aitheta/index/offline_index_attr.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"

namespace indexlib {
namespace aitheta_plugin {

enum class LoadType { kUnknown = 0, kToReduce, kToSearch };

class Segment {
 public:
    Segment(const SegmentMeta &meta) : mMeta(meta){};
    virtual ~Segment() = default;

 private:
    Segment(const Segment &) = delete;
    Segment &operator=(const Segment &) = delete;

 public:
    virtual bool Load(const indexlib::file_system::DirectoryPtr &dir, const LoadType &ty) = 0;
    virtual bool Dump(const indexlib::file_system::DirectoryPtr &dir) = 0;
    virtual EmbedHolderIterator CreateEmbedHolderIterator(const util::KeyValueMap &keyVal,
                                                          const std::set<catid_t> &validCatids) = 0;

 public:
    virtual void SetDocIdMap(const DocIdMap &docIdMap) { mDocIdMapPtr.reset(new DocIdMap(docIdMap)); }
    const DocIdMapPtr &GetDocIdMap() const { return mDocIdMapPtr; }

    void SetOfflineKnnConfig(const KnnConfig &knnCfg) { mOfflineKnnCfg = knnCfg; }
    const KnnConfig &GetOfflineKnnConfig() const { return mOfflineKnnCfg; }

    SegmentType GetType() const { return mMeta.GetType(); }
    const SegmentMeta &GetMeta() const { return mMeta; }

    const OfflineIndexAttrHolder &GetOfflineIndexAttrHolder() const { return mOfflineIndexAttrHolder; }

 public:
    indexlib::file_system::FileWriterPtr
        static CreateIndexWriter(const indexlib::file_system::DirectoryPtr &dir, bool deleteIfExisted = true) {
        if (deleteIfExisted) {
            indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
            dir->RemoveFile(INDEX_FILE, removeOption);
        }
        return IndexlibIoWrapper::CreateWriter(dir, INDEX_FILE);
    }

    indexlib::file_system::FileReaderPtr static CreateIndexProvider(
        const indexlib::file_system::DirectoryPtr &dir,
        const indexlib::file_system::FSOpenType &type = indexlib::file_system::FSOT_BUFFERED) {
        return IndexlibIoWrapper::CreateReader(dir, INDEX_FILE, type);
    }

 protected:
    bool DumpOfflineIndexAttrHolder(const indexlib::file_system::DirectoryPtr &dir) {
        return mOfflineIndexAttrHolder.Dump(dir);
    }
    bool LoadOfflineIndexAttrHolder(const indexlib::file_system::DirectoryPtr &dir) {
        return mOfflineIndexAttrHolder.Load(dir);
    }
    bool DumpOfflineKnnCfg(const indexlib::file_system::DirectoryPtr &dir) { return mOfflineKnnCfg.Dump(dir); }
    bool LoadOfflineKnnCfg(const indexlib::file_system::DirectoryPtr &dir) { return mOfflineKnnCfg.Load(dir); }

 protected:
    SegmentMeta mMeta;
    KnnConfig mOfflineKnnCfg;
    OfflineIndexAttrHolder mOfflineIndexAttrHolder;
    DocIdMapPtr mDocIdMapPtr;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Segment);

}
}

#endif  //  INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEGMENT_H_
