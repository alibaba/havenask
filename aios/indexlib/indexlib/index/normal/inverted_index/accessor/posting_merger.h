#ifndef __INDEXLIB_POSTING_MERGER_H
#define __INDEXLIB_POSTING_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_writer.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_term_info.h"
#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"

DECLARE_REFERENCE_CLASS(index, PostingIterator);

IE_NAMESPACE_BEGIN(index);

class PostingMerger
{
public:
    PostingMerger() : mTermPayload(0) {};
    virtual ~PostingMerger() {};
    
public:
    virtual void Merge(const SegmentTermInfos& segTermInfos, 
                       const ReclaimMapPtr& reclaimMap) = 0;

    virtual void SortByWeightMerge(const SegmentTermInfos& segTermInfos, 
                                   const ReclaimMapPtr& reclaimMap) = 0;

    virtual void Dump(dictkey_t key, IndexOutputSegmentResources& indexOutputSegmentResources) = 0;    
    virtual df_t GetDocFreq() = 0;
    virtual ttf_t GetTotalTF() = 0;

//    virtual uint64_t GetDumpLength() const { return 0; }
    virtual uint64_t GetPostingLength() const = 0;
    virtual bool GetDictInlinePostingValue(uint64_t& inlinePostingValue) { return false; }

    virtual bool IsCompressedPostingHeader() const = 0;
    termpayload_t GetTermPayload() const
    {
        return mTermPayload;
    }

    virtual PostingIteratorPtr CreatePostingIterator() = 0;

protected:
    termpayload_t mTermPayload;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_MERGER_H
