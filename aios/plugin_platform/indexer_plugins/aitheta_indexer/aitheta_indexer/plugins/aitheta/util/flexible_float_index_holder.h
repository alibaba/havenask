#ifndef __AITHETA_FLEXIBLE_FLOATmIndexmHolder_H__
#define __AITHETA_FLEXIBLE_FLOATmIndexmHolder_H__

#include <memory>

#include <aitheta/index_holder.h>
#include <aitheta/algorithm/mips_reformer.h>
#include "aitheta_indexer/plugins/aitheta/common_define.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
/*!
 * FLexible Float Index Holder
 */
class FlexibleFloatIndexHolder : public aitheta::IndexHolder {
    friend class AithetaIndexReducer;

 public:
    typedef std::shared_ptr<FlexibleFloatIndexHolder> Pointer;

 public:
    class Iterator : public aitheta::IndexHolder::Iterator {
     public:
        //! Constructor
        Iterator(const FlexibleFloatIndexHolder *holder) : mIndex(0), mHolder(holder) {}
        //! Retrieve pointer of data
        virtual const void *data(void) const {
            return static_cast<const void *>(mHolder->mDataVecor[mIndex].mEmbedding.get());
        }
        //! Test if the iterator is valid
        virtual bool isValid(void) const { return (mIndex < mHolder->mDataVecor.size()); }
        //! Retrieve primary key
        virtual uint64_t key(void) const { return static_cast<uint64_t>(mHolder->mDataVecor[mIndex].mDocId); }
        //! Next iterator
        virtual void next(void) { ++mIndex; }
        //! Reset the iterator
        virtual void reset(void) { mIndex = 0; }

     private:
        size_t mIndex;
        const FlexibleFloatIndexHolder *mHolder;
        Iterator(const Iterator &) = delete;
        Iterator &operator=(const Iterator &) = delete;
    };

 public:
    FlexibleFloatIndexHolder(int dim) : mDimension(dim) {}
    ~FlexibleFloatIndexHolder() = default;

 public:
    bool mipsReform(const MipsParams &inputParams, MipsParams &outputParams);
    //! Retrieve count of elements in holder
    virtual size_t count(void) const { return mDataVecor.size(); }
    //! Retrieve dimension
    virtual size_t dimension(void) const { return mDimension + mMipsMval; }
    //! Retrieve type information
    virtual IndexHolder::FeatureTypes type(void) const { return IndexHolder::kTypeFloat; }
    //! Create a new iterator
    virtual IndexHolder::Iterator::Pointer createIterator(void) const {
        return IndexHolder::Iterator::Pointer(new FlexibleFloatIndexHolder::Iterator(this));
    }
    //! Request a change in capacity
    void reserve(size_t size) { mDataVecor.reserve(size); }
    //! Append an element into holder
    void push_back(EmbeddingInfo &&embeddingData) { mDataVecor.push_back(embeddingData); }
    void push_back(EmbeddingInfo &embeddingData) { mDataVecor.push_back(embeddingData); }
    void emplace_back(docid_t docid, std::shared_ptr<float> &embedding) { mDataVecor.emplace_back(docid, embedding); }
    void clear();

 private:
    std::vector<EmbeddingInfo> mDataVecor;
    int mDimension;
    int mMipsMval = 0;
    IE_LOG_DECLARE();
};

typedef FlexibleFloatIndexHolder::Pointer FlexibleFloatIndexHolderPtr;

IE_NAMESPACE_END(aitheta_plugin);

#endif  // __AITHETA_FLOATmIndexmHolder_H__
