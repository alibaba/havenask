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

#include "indexlib_plugin/plugins/aitheta/util/embed_holder.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include <aitheta/utility/vector.h>

namespace indexlib {
namespace aitheta_plugin {
using namespace aitheta;
IE_LOG_SETUP(aitheta_plugin, EmbedHolder);

bool EmbedHolder::DoMips(const aitheta::MipsReformer &src, aitheta::MipsReformer &dst) {
    MipsParams in(true, src.m(), src.u(), src.norm());
    MipsParams out;
    if (!DoMips(in, out)) {
        return false;
    }
    dst = MipsReformer(out.mval, out.uval, out.norm);
    return true;
}

bool EmbedHolder::DoMips(const MipsParams &inputParams, MipsParams &outputParams) {
    if (!DoLazyLoad()) {
        IE_LOG(ERROR, "failed to do mips reform due to failure of lazy load");
        return false;
    }

    if (mMipsReformDone) {
        IE_LOG(ERROR, "the data has already been reformed");
        return false;
    }
    if (!inputParams.enable) {
        IE_LOG(ERROR, "trying to DoMips while inputParams.enabled is false");
        return false;
    }

    int32_t mval = inputParams.mval;

    IE_LOG(INFO, "inputParams:[enable:[%d], mval:[%u], uval:[%f], norm:[%f]]", inputParams.enable, inputParams.mval,
           inputParams.uval, inputParams.norm);
    aitheta::MipsReformer refomer(inputParams.mval, inputParams.uval, inputParams.norm);
    if (inputParams.norm <= 0) {
        IE_LOG(INFO, "begin norm update with data size:[%lu], dimension:[%d]", mDataVec.size(), mDimension);
        for (const auto &embedInfo : mDataVec) {
            refomer.update(embedInfo.embedding.get(), mDimension);
        }
        outputParams = inputParams;
        outputParams.norm = refomer.norm();  // outputParams use updated norm
        IE_LOG(INFO, "finished norm update: updated norm[%f]", refomer.norm());
    }

    IE_LOG(INFO, "begin transFeature with data size:[%lu], dimension:[%d]", mDataVec.size(), mDimension);
    aitheta::Vector<float> vec;
    for (auto &embedInfo : mDataVec) {
        refomer.transFeature(embedInfo.embedding.get(), mDimension, &vec);
        assert(vec.size() == size_t(mDimension + mval));
        MALLOC(embedInfo.embedding, vec.size(), float);
        std::copy(vec.data(), vec.data() + vec.size(), embedInfo.embedding.get());
    }

    mDimension += mval;
    mMipsReformDone = true;
    IE_LOG(INFO, "finished transFeature, new dimension[%d], norm[%f], uval[%f]", mDimension, refomer.norm(),
           refomer.u());
    return true;
}

bool EmbedHolder::UndoMips(const MipsReformer &reformer) {
    if (!mMipsReformDone) {
        IE_LOG(ERROR, "no need to do revert mips reform");
        return false;
    }

    if (!DoLazyLoad()) {
        return false;
    }

    aitheta::Vector<float> vec;
    for (auto &embedInfo : mDataVec) {
        reformer.revertFeature(embedInfo.embedding.get(), mDimension, &vec);
        std::copy(vec.data(), vec.data() + vec.size(), embedInfo.embedding.get());
    }

    mDimension -= reformer.m();
    mMipsReformDone = false;
    IE_LOG(INFO, "finish reverting with param dim:[%d], m:[%d], u:[%f], norm:[%f]", mDimension, reformer.m(),
           reformer.u(), reformer.norm());
    return true;
}

bool EmbedHolder::Load(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size,
                       bool lazyLoad) {
    assert(fr);
    int64_t catid = 0l;
    fr->Read(&catid, sizeof(catid), offset).GetOrThrow();
    if (catid != mCatid) {
        IE_LOG(ERROR, "failed to load, expect catid[%ld], actual[%ld]", mCatid, catid);
        return false;
    }
    offset += sizeof(catid);
    size -= sizeof(catid);

    size_t num = 0u;
    fr->Read(&num, sizeof(num), offset).GetOrThrow();
    offset += sizeof(catid);
    size -= sizeof(catid);

    if (lazyLoad) {
        SetLazyLoadState(fr, offset, size);
    } else {
        if (!InnerLoad(fr, offset, size)) {
            return false;
        }
        IE_LOG(DEBUG, "load for catid[%ld], in path[%s]", mCatid, fr->DebugString().c_str());
    }

    return true;
}

bool EmbedHolder::Dump(const indexlib::file_system::FileWriterPtr &fw, uint64_t &offset, uint64_t &size) {
    assert(fw);
    offset = fw->GetLength();
    fw->Write(&mCatid, sizeof(mCatid)).GetOrThrow();

    size_t num = mDataVec.size();
    fw->Write(&num, sizeof(num)).GetOrThrow();

    for (const auto &embedInfo : mDataVec) {
        auto docid = embedInfo.docid;
        fw->Write(&docid, sizeof(docid)).GetOrThrow();
        auto embedding = embedInfo.embedding;
        if (!embedding) {
            IE_LOG(ERROR, "embedding is nullptr for docid[%d]", docid);
            return false;
        }
        fw->Write(embedding.get(), sizeof(float) * mDimension).GetOrThrow();
    }
    size = fw->GetLength() - offset;
    IE_LOG(DEBUG, "dump for catid[%ld], number[%lu] in path[%s]", mCatid, num, fw->DebugString().c_str());
    return true;
}

bool EmbedHolder::InnerLoad(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size) const {
    size_t docNum = size / (sizeof(docid_t) + sizeof(float) * mDimension);
    size_t nextOffset = offset;
    for (size_t i = 0; i < docNum; ++i) {
        docid_t docid;
        EmbeddingPtr embedding;
        MALLOC(embedding, mDimension, float);
        if (!embedding) {
            return false;
        }
        fr->Read(&docid, sizeof(docid), nextOffset).GetOrThrow();
        nextOffset += sizeof(docid);
        fr->Read(embedding.get(), sizeof(float) * mDimension, nextOffset).GetOrThrow();
        nextOffset += sizeof(float) * mDimension;
        mDataVec.emplace_back(docid, embedding);
    }
    return true;
}

bool EmbedHolder::DoLazyLoad() const {
    if (mLazyLoad.enable && !mLazyLoad.loaded) {
        if (!InnerLoad(mLazyLoad.fr, mLazyLoad.offset, mLazyLoad.size)) {
            return false;
        }
        mLazyLoad.loaded = true;
    }
    return true;
}

void EmbedHolder::SetLazyLoadState(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size) {
    mLazyLoad.enable = true;
    mLazyLoad.loaded = false;
    mLazyLoad.fr = fr;
    mLazyLoad.offset = offset;
    mLazyLoad.size = size;
    IE_LOG(DEBUG, "enable lazy load for catid[%ld]", mCatid);
}

void EmbedHolder::ResetLazyLoadState() {
    mLazyLoad.enable = false;
    mLazyLoad.loaded = false;
    mLazyLoad.size = 0u;
    mLazyLoad.offset = 0u;
    if (mLazyLoad.fr) {
        mLazyLoad.fr.reset();
    }
}

IndexHolder::Iterator::Pointer EmbedHolder::createIterator(void) const {
    if (!DoLazyLoad()) {
        return IndexHolder::Iterator::Pointer();
    }
    return IndexHolder::Iterator::Pointer(new EmbedHolder::Iterator(this));
}

std::vector<EmbedInfo> &EmbedHolder::GetEmbedVec() {
    DoLazyLoad();
    return mDataVec;
}

}
}
