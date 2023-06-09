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
#include "indexlib_plugin/plugins/aitheta/rt_segment.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/output_storage.h"

using namespace autil;
using namespace aitheta;
using namespace std;
using namespace indexlib::file_system;
namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, RtSegment);

bool RtSegment::Load(const indexlib::file_system::DirectoryPtr& dir, const LoadType& ty) {
    INDEXLIB_FATAL_ERROR(UnImplement, "RtSegment does not support the function");
    return false;
}

size_t RtSegment::Size() {
    size_t size = 0u;
    if (!mOnlineIndexHolder) {
        return size;
    }
    // TODO(richard.sy) optimize it
    for (auto& catid2Index : mOnlineIndexHolder->Get()) {
        auto index = DYNAMIC_POINTER_CAST(RtIndex, catid2Index.second);
        assert(index);
        auto& stats = index->streamer->stats();
        size += stats.getIndexSize();
    }
    return size;
}

bool RtSegment::Dump(const indexlib::file_system::DirectoryPtr& dir) {
    IE_LOG(INFO, "start dumping rt sgt in dir[%s]", dir->DebugString().c_str());

    mIndexWriter = Segment::CreateIndexWriter(dir);
    if (!mIndexWriter) {
        return false;
    }
    mOfflineIndexAttrHolder.Reset();
    if (IsAvailable()) {
        for (auto& catid2Index : mOnlineIndexHolder->Get()) {
            if (!DumpIndex(catid2Index.first, catid2Index.second)) {
                mIndexWriter->Close().GetOrThrow();
                return false;
            }
        }
    }
    mIndexWriter->Close().GetOrThrow();

    if (!DumpOfflineIndexAttrHolder(dir)) {
        return false;
    }
    if (!DumpOfflineKnnCfg(dir)) {
        return false;
    }
    if (!DumpMeta(dir)) {
        return false;
    }

    IE_LOG(INFO, "finish dump rt sgt in dir[%s]", dir->DebugString().c_str());
    return true;
}

bool RtSegment::Add(int64_t catid, docid_t docid, EmbeddingPtr embedding) {
    if (!IsAvailable()) {
        return false;
    }

    OnlineIndexPtr index;
    if (!mOnlineIndexHolder->Get(catid, index)) {
        IE_LOG(ERROR, "catid[%ld] doesn't appear in full/inc sgt", catid);
        return false;
    }

    if (!index->Build(docid, embedding, mSchemaParam.dimension)) {
        return false;
    }
    ++mDocCount;
    return true;
}

EmbedHolderIterator RtSegment::CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                         const std::set<catid_t>& validCatids) {
    INDEXLIB_FATAL_ERROR(UnImplement, "RtSegment does not support the function");
    return EmbedHolderIterator();
}

bool RtSegment::DumpIndex(catid_t catid, const OnlineIndexPtr& onlineIndex) {
    if (onlineIndex->docNum <= 0) {
        IE_LOG(DEBUG, "index is empty in cat[%ld]", catid);
        return true;
    }

    auto index = DYNAMIC_POINTER_CAST(RtIndex, onlineIndex);
    if (!index) {
        IE_LOG(ERROR, "failed to dynamic_cast to RtIndex");
        return false;
    }

    auto& streamer = index->streamer;
    IndexStoragePtr storage(new OutputStorage(mIndexWriter));

    size_t offset = mIndexWriter->GetLength();
    int32_t ret = streamer->dumpIndex(TimeUtility::currentTime(), ".", storage);
    if (ret != 0) {
        IE_LOG(ERROR, "failed to dump rt sgt, error[%s]", IndexError::What(ret));
        return false;
    }

    size_t indexSize = mIndexWriter->GetLength() - offset;
    mOfflineIndexAttrHolder.Add(catid, *index, offset, indexSize);

    return true;
}

bool RtSegment::DumpMeta(const DirectoryPtr& dir) {
    mMeta.SetType(SegmentType::kIndex);
    mMeta.SetDimension(mSchemaParam.dimension);
    mMeta.SetDocNum(mDocCount);
    mMeta.SetCatNum(mOnlineIndexHolder->Get().size());
    mMeta.IsBuiltOnline(true);
    return mMeta.Dump(dir);
}

}
}
