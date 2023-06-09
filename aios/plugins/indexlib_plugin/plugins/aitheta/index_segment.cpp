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
#include "indexlib_plugin/plugins/aitheta/index_segment.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_reducer.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/idx_sgt_holder_iterator_impl.h"
#include "indexlib_plugin/plugins/aitheta/util/parallel_merge/parallel_merge_util.h"

namespace indexlib {
namespace aitheta_plugin {
using namespace indexlib::file_system;
using namespace indexlib::index;
IE_LOG_SETUP(aitheta_plugin, IndexSegment);

bool IndexSegment::Dump(const DirectoryPtr& dir) {
    IE_LOG(INFO, "start dumping index in dir[%s]", dir->DebugString().c_str());

    if (mIndexWriter) {
        mIndexWriter->Close().GetOrThrow();
    }

    if (!DumpOfflineIndexAttrHolder(dir)) {
        return false;
    }
    if (!DumpOfflineKnnCfg(dir)) {
        return false;
    }

    mMeta.SetCatNum(mOfflineIndexAttrHolder.Size());
    size_t docNum = 0u;
    for (size_t idx = 0u; idx < mMeta.GetCatNum(); ++idx) {
        docNum += mOfflineIndexAttrHolder.Get(idx).docNum;
    }
    mMeta.SetDocNum(docNum);
    if (!mMeta.Dump(dir)) {
        return false;
    }

    IE_LOG(INFO, "finsih dumping index in dir[%s]", dir->DebugString().c_str());
    return true;
}

bool IndexSegment::Load(const DirectoryPtr& dir, const LoadType& loadType) {
    auto indexTy = mMeta.GetType();

    switch (indexTy) {
        case SegmentType::kIndex: {
            return InnerLoad(dir, loadType);
        }
        case SegmentType::kPMIndex: {
            ParallelReduceMeta parallelReduceMeta;
            if (!parallelReduceMeta.Load(dir)) {
                return false;
            }
            if (!mOfflineIndexAttrHolder.Load(dir)) {
                return false;
            }
            return ParallelMergeUtil::LoadIndexSegments(dir, parallelReduceMeta, loadType, mSubSegments);
        }
        default: {
            IE_LOG(INFO, "unexpected index type [%s]", Type2Str(indexTy).c_str());
            return false;
        }
    }
}

indexlib::file_system::FileWriterPtr IndexSegment::CreateIndexWriter(
    const indexlib::file_system::DirectoryPtr& dir, bool deleteIfExisted) {
    if (mIndexWriter) {
        INDEXLIB_FATAL_ERROR(Exist, "index writer exists in directory[%s]", dir->DebugString().c_str());
    }
    mIndexWriter = Segment::CreateIndexWriter(dir, deleteIfExisted);
    if (!mIndexWriter) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "failed to create index writer");
    }
    return mIndexWriter;
}

void IndexSegment::SetDocIdMap(const DocIdMap &docIdMap) {
    mDocIdMapPtr.reset(new DocIdMap(docIdMap));
    for (auto& segment : mSubSegments) {
        segment->SetDocIdMap(docIdMap);
    }
}

EmbedHolderIterator IndexSegment::CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                            const std::set<catid_t>& validCatids) {
    auto indexTy = mMeta.GetType();
    switch (indexTy) {
        case SegmentType::kIndex: {
            return InnerCreateEmbedHolderIterator(keyVal, validCatids);
        }
        case SegmentType::kPMIndex: {
            EmbedHolderIterator iterator;
            for (size_t idx = 0u; idx < mSubSegments.size(); ++idx) {
                auto& sgt = mSubSegments[idx];
                if (idx == 0u) {
                    iterator = sgt->CreateEmbedHolderIterator(keyVal, validCatids);
                } else {
                    auto iter = sgt->CreateEmbedHolderIterator(keyVal, validCatids);
                    if (!iterator.Merge(iter)) {
                        return EmbedHolderIterator(mMeta.GetDimension());
                    }
                }
            }
            return iterator;
        }
        default: {
            IE_LOG(ERROR, "unexpected index type[%s]", Type2Str(indexTy).c_str());
            return EmbedHolderIterator(mMeta.GetDimension());
        }
    }
}

bool IndexSegment::InnerLoad(const DirectoryPtr& dir, const LoadType& loadType) {
    IE_LOG(INFO, "start loading index in dir[%s]", dir->DebugString().c_str());

    FSOpenType openType = FSOT_UNKNOWN;
    switch (loadType) {
        case LoadType::kToSearch:
            openType = FSOT_MMAP;
            break;
        case LoadType::kToReduce:
            openType = FSOT_BUFFERED;
            break;
        default:
            IE_LOG(ERROR, "unknown index load type");
            return false;
    }

    mIndexProvider = CreateIndexProvider(dir, openType);
    if (!mIndexProvider) {
        return false;
    }

    if (0u == mIndexProvider->GetLength()) {
        IE_LOG(WARN, "index file is empty");
    }
    if (!mOfflineIndexAttrHolder.Load(dir)) {
        return false;
    }

    LoadOfflineKnnCfg(dir);

    IE_LOG(INFO, "finish loading index in dir[%s]", dir->DebugString().c_str());
    return true;
}

EmbedHolderIterator IndexSegment::InnerCreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                                 const std::set<catid_t>& validCatids) {
    if (!mIndexProvider) {
        IE_LOG(ERROR, "index provider is not initialized");
        return EmbedHolderIterator();
    }

    auto iteratorImpl =
        new IdxSgtHolderIteratorImpl(mIndexProvider, mOfflineKnnCfg, keyVal, mOfflineIndexAttrHolder, mDocIdMapPtr);
    return EmbedHolderIterator(mMeta.GetDimension(), iteratorImpl, validCatids);
}

}
}
