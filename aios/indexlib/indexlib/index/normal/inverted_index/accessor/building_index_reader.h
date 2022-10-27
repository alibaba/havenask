#ifndef __INDEXLIB_BUILDING_INDEX_READER_H
#define __INDEXLIB_BUILDING_INDEX_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/accessor/segment_posting.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

IE_NAMESPACE_BEGIN(index);

class BuildingIndexReader
{
public:
    BuildingIndexReader(const PostingFormatOption& postingFormatOption)
        : mPostingFormatOption(postingFormatOption)
    {}
    
    ~BuildingIndexReader() {}
    
public:
    void AddSegmentReader(docid_t baseDocId,
                          const IndexSegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader)
        {
            mInnerSegReaders.push_back(std::make_pair(baseDocId, inMemSegReader));
        }
    }

    void GetSegmentPosting(dictkey_t key, 
                           std::vector<index::SegmentPosting>& segPostings,
                           autil::mem_pool::Pool* sessionPool) const
    {
        for (size_t i = 0; i < mInnerSegReaders.size(); ++i)
        {
            index::SegmentPosting segPosting(mPostingFormatOption);
            if (mInnerSegReaders[i].second->GetSegmentPosting(
                            key, mInnerSegReaders[i].first, segPosting, sessionPool))
            {
                segPostings.push_back(std::move(segPosting));
            }
        }
    }

    size_t GetSegmentCount() const { return mInnerSegReaders.size(); }

protected:
    typedef std::pair<docid_t, IndexSegmentReaderPtr> SegmentReaderItem;
    typedef std::vector<SegmentReaderItem> SegmentReaders;

    SegmentReaders mInnerSegReaders;
    PostingFormatOption mPostingFormatOption;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BuildingIndexReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BUILDING_INDEX_READER_H
