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
#include "indexlib_plugin/plugins/aitheta/raw_segment.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"
#include "indexlib_plugin/plugins/aitheta/util/embed_holder_iterator/raw_sgt_holder_iterator_impl.h"
namespace indexlib {
namespace aitheta_plugin {
using namespace indexlib::file_system;
using namespace std;
IE_LOG_SETUP(aitheta_plugin, RawSegment);

bool RawSegment::Dump(const DirectoryPtr& dir) {
    if (!DumpEmbed(dir)) {
        return false;
    }
    if (!DumpOfflineIndexAttrHolder(dir)) {
        return false;
    }
    if (!DumpMeta(dir)) {
        return false;
    }
    IE_LOG(INFO, "finish dumping segment in path[%s]", dir->DebugString().c_str());
    return true;
}

bool RawSegment::Load(const DirectoryPtr& dir, const LoadType& ty) {
    if (ty != LoadType::kToReduce) {
        IE_LOG(ERROR, "only support reduce for raw sgt Load");
        return false;
    }
    if (!LoadOfflineIndexAttrHolder(dir)) {
        return false;
    }
    if (!LoadEmbed(dir)) {
        return false;
    }
    IE_LOG(INFO, "finish loading segment in path[%s]", dir->DebugString().c_str());
    return true;
}

bool RawSegment::Add(catid_t catid, docid_t docid, embedding_t embedding) {
    EmbedHolderPtr embedHolder;
    auto it = mCatid2EmbedHolder.find(catid);
    if (it != mCatid2EmbedHolder.end()) {
        embedHolder = it->second;
    } else {
        EmbedHolderPtr newEmbedHolder(new EmbedHolder(mMeta.GetDimension(), catid));
        mCatid2EmbedHolder.emplace(catid, newEmbedHolder);
        embedHolder = newEmbedHolder;
        mSize += sizeof(catid) + sizeof(EmbedHolder);
    }

    embedHolder->Add(docid, embedding);
    mSize += sizeof(docid_t) + sizeof(float) * mMeta.GetDimension();
    if (mMaxDocCount < embedHolder->count()) {
        mMaxDocCount = embedHolder->count();
    }
    return true;
}

EmbedHolderIterator RawSegment::CreateEmbedHolderIterator(const util::KeyValueMap& keyVal,
                                                          const std::set<catid_t>& validCatids) {
    auto rawHolderIterImpl = new RawSgtHolderIteratorImpl(mCatid2EmbedHolder, mDocIdMapPtr);
    return EmbedHolderIterator(mMeta.GetDimension(), rawHolderIterImpl, validCatids);
}

bool RawSegment::DumpEmbed(const DirectoryPtr& dir) {
    auto mEmbedWriter = CreateEmbedWriter(dir);
    if (!mEmbedWriter) {
        return false;
    }
    for (const auto& kv : mCatid2EmbedHolder) {
        int64_t catid = kv.first;
        const auto& embedHolder = kv.second;
        if (!embedHolder->count()) {
            continue;
        }
        uint64_t offset = 0ul;
        uint64_t size = 0ul;
        if (!embedHolder->Dump(mEmbedWriter, offset, size)) {
            return false;
        }
        mOfflineIndexAttrHolder.Add(catid, embedHolder->count(), offset, size);
    }
    mEmbedWriter->Close().GetOrThrow();
    return true;
}

bool RawSegment::DumpMeta(const DirectoryPtr& dir) {
    mMeta.SetType(SegmentType::kRaw);
    mMeta.SetCatNum(mCatid2EmbedHolder.size());
    size_t docNum = 0u;
    for_each(mCatid2EmbedHolder.begin(), mCatid2EmbedHolder.end(),
             [&](const std::pair<int64_t, EmbedHolderPtr>& kv) { docNum += kv.second->count(); });
    mMeta.SetDocNum(docNum);
    if (mMeta.GetDimension() == 0u) {
        IE_LOG(ERROR, "dimension shoule be set, failed to dump meta");
        return false;
    }
    return mMeta.Dump(dir);
}

bool RawSegment::LoadEmbed(const DirectoryPtr& dir) {
    mEmbedProvider = CreateEmbedProvider(dir);
    if (!mEmbedProvider) {
        return false;
    }
    for (size_t idx = 0; idx < mOfflineIndexAttrHolder.Size(); ++idx) {
        auto& indexAttr = mOfflineIndexAttrHolder.Get(idx);
        int64_t catid = indexAttr.catid;
        EmbedHolderPtr embedHolder(new EmbedHolder(mMeta.GetDimension(), catid));
        bool lazyLoad = true;
        if (!embedHolder->Load(mEmbedProvider, indexAttr.offset, indexAttr.size, lazyLoad)) {
            IE_LOG(ERROR, "failed to read embedding of category[%ld]", catid);
            return false;
        }
        mCatid2EmbedHolder.emplace(catid, embedHolder);
    }
    return true;
}

const std::set<catid_t> RawSegment::GetCatids() const {
    std::set<catid_t> catids;
    for_each(mCatid2EmbedHolder.begin(), mCatid2EmbedHolder.end(),
             [&](const std::pair<catid_t, EmbedHolderPtr>& kv) { catids.insert(kv.first); });
    return catids;
}

}
}
