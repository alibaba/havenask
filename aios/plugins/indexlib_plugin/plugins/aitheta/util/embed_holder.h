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
#ifndef __INDEXLIB_PLUGIN_PLUGINS_UTIL_FLEXIBLE_FLOAT_INDEX_HOLDER_H
#define __INDEXLIB_PLUGIN_PLUGINS_UTIL_FLEXIBLE_FLOAT_INDEX_HOLDER_H

#include <memory>

#include <aitheta/index_holder.h>
#include <aitheta/algorithm/mips_reformer.h>
#include "indexlib/file_system/file/BufferedFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

struct LazyLoad {
    bool enable = false;
    bool loaded = false;
    size_t offset = 0u;
    size_t size = 0u;
    indexlib::file_system::FileReaderPtr fr;
};

// inherit proxima IndexHolder
class EmbedHolder : public aitheta::IndexHolder {
    friend class AithetaIndexReducer;

 public:
    typedef std::shared_ptr<EmbedHolder> Pointer;

 public:
    class Iterator : public aitheta::IndexHolder::Iterator {
     public:
        //! Constructor
        Iterator(const EmbedHolder *holder) : mIndex(0), mHolder(holder) {}
        //! Retrieve pointer of data
        virtual const void *data(void) const {
            return static_cast<const void *>(mHolder->mDataVec[mIndex].embedding.get());
        }
        //! Test if the iterator is valid
        virtual bool isValid(void) const { return (mIndex < mHolder->mDataVec.size()); }
        //! Retrieve primary key
        virtual uint64_t key(void) const { return static_cast<uint64_t>(mHolder->mDataVec[mIndex].docid); }
        //! Next iterator
        virtual void next(void) { ++mIndex; }
        //! Reset the iterator
        virtual void reset(void) { mIndex = 0; }

     private:
        size_t mIndex;
        const EmbedHolder *mHolder;
        Iterator(const Iterator &) = delete;
        Iterator &operator=(const Iterator &) = delete;
    };

 public:
    EmbedHolder(int dim, int64_t catid = DEFAULT_CATEGORY_ID, bool done = false)
        : mDimension(dim), mCatid(catid), mMipsReformDone(done) {}
    ~EmbedHolder() = default;

 public:
    //! Retrieve count of elements in holder
    virtual size_t count(void) const { return mDataVec.size(); }
    //! Retrieve dimension
    virtual size_t dimension(void) const { return mDimension; }
    //! Retrieve type information
    virtual IndexHolder::FeatureTypes type(void) const { return IndexHolder::kTypeFloat; }
    //! Create a new iterator
    virtual IndexHolder::Iterator::Pointer createIterator(void) const;

 public:
    void Add(docid_t docid, const std::shared_ptr<float> &embedding) { mDataVec.emplace_back(docid, embedding); }
    void BatchAdd(const std::vector<EmbedInfo> &vec) { mDataVec.insert(mDataVec.end(), vec.begin(), vec.end()); }
    // @depreciate
    bool DoMips(const MipsParams &inputParams, MipsParams &outputParams);
    bool DoMips(const aitheta::MipsReformer &src, aitheta::MipsReformer &dst);
    bool UndoMips(const aitheta::MipsReformer &reformer);
    int64_t GetCatid() const { return mCatid; }
    bool Dump(const indexlib::file_system::FileWriterPtr &fw, uint64_t &offset, uint64_t &size);
    bool Load(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size,
              bool lazyLoad = false);
    std::vector<EmbedInfo> &GetEmbedVec();

 private:
    bool InnerLoad(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size) const;
    bool DoLazyLoad() const;
    void SetLazyLoadState(const indexlib::file_system::FileReaderPtr &fr, uint64_t offset, uint64_t size);
    void ResetLazyLoadState();

 private:
    mutable std::vector<EmbedInfo> mDataVec;
    mutable LazyLoad mLazyLoad;
    int32_t mDimension;
    int64_t mCatid;
    bool mMipsReformDone;
    IE_LOG_DECLARE();
};

typedef EmbedHolder::Pointer EmbedHolderPtr;

}
}

#endif  // __INDEXLIB_PLUGIN_PLUGINS_UTIL_FLEXIBLE_FLOAT_INDEX_HOLDER_H
