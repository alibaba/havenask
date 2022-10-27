#ifndef __INDEXLIB_TERM_POSTING_INFO_H
#define __INDEXLIB_TERM_POSTING_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/byte_slice_reader.h"

IE_NAMESPACE_BEGIN(index);

class TermPostingInfo
{
public:
    TermPostingInfo();
    ~TermPostingInfo();
public:
    uint32_t GetPostingSkipSize() const {return mPostingSkipSize;}
    void SetPostingSkipSize(uint32_t postingSkipSize)
    {
        mPostingSkipSize = postingSkipSize;
    }
    uint32_t GetPostingListSize() const {return mPostingListSize;}
    void SetPostingListSize(uint32_t postingListSize)
    {
        mPostingListSize = postingListSize;
    }
    uint32_t GetPositionSkipSize() const {return mPositionSkipSize;}
    void SetPositionSkipSize(uint32_t positionSkipSize)
    {
        mPositionSkipSize = positionSkipSize;
    }
    uint32_t GetPositionListSize() const {return mPositionListSize;}
    void SetPositionListSize(uint32_t positionListSize)
    {
        mPositionListSize = positionListSize;
    }
    void LoadPosting(common::ByteSliceReader* sliceReader);
    void LoadPositon(common::ByteSliceReader* sliceReader);
private:
    uint32_t mPostingSkipSize;
    uint32_t mPostingListSize;
    uint32_t mPositionSkipSize;
    uint32_t mPositionListSize;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TermPostingInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TERM_POSTING_INFO_H
