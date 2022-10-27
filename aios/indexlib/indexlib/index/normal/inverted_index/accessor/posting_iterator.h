#ifndef __INDEXLIB_POSTING_ITERATOR_H
#define __INDEXLIB_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/common/error_code.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/inverted_index/accessor/term_posting_info.h"

DECLARE_REFERENCE_CLASS(index, TermMeta);
DECLARE_REFERENCE_CLASS(index, TermMatchData);

IE_NAMESPACE_BEGIN(index);

class PostingIterator
{
public:
    virtual ~PostingIterator() {}
public:
    /**
     * Get term meta information of the posting-list
     * @return internal term meta object
     */
    virtual index::TermMeta* GetTermMeta() const = 0;

    /**
     * Get truncate term meta information of the posting list
     * @return when no truncate posting, return main chain term meta.
     */
    virtual index::TermMeta* GetTruncateTermMeta() const
    {
        return GetTermMeta();
    }

    /**
     * Get match value
     */
    virtual MatchValueType GetMatchValueType() const
    {
        return mv_unknown;
    }
    
    /**
     * Get match value
     */
    virtual matchvalue_t GetMatchValue() const
    {
        return matchvalue_t();
    }

    /**
     * Find the docid which is equal to or greater than /docId/ in posting-list
     */
    virtual docid_t SeekDoc(docid_t docId) = 0;

    virtual common::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) = 0;

    /**
     * Return true if position-list exists
     */
    virtual bool HasPosition() const = 0;

    /**
     * Find a in-doc position which is equal to or greater
     * than /pos/ in position-list
     */
    pos_t SeekPosition(pos_t pos)
    {
        pos_t result = INVALID_POSITION;
        auto ec = SeekPositionWithErrorCode(pos, result);
        common::ThrowIfError(ec);
        return result;
    }
    
    virtual common::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t &nextPos)
    {
        nextPos = INVALID_POSITION;
        return common::ErrorCode::OK;
    }

    /**
     * Extract posting data for ranking
     */
    virtual void Unpack(TermMatchData& termMatchData) = 0;

    /**
     * Get doc payload of current sought doc
     */
    virtual docpayload_t GetDocPayload()
    {
        return 0;
    }

    /**
     * Get type
     */
    virtual PostingIteratorType GetType() const
    {
        return pi_unknown;
    }
    
    virtual index::TermPostingInfo GetTermPostingInfo() const
    {
        return index::TermPostingInfo();
    }

    virtual void Reset() = 0;

    virtual PostingIterator* Clone() const = 0;
    
    virtual autil::mem_pool::Pool *GetSessionPool() const {
        return NULL;
    }

public:
    
    common::ErrorCode UnpackWithErrorCode(TermMatchData& termMatchData)
    {
        try
        {
            Unpack(termMatchData);
            return common::ErrorCode::OK;
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }
    
    common::ErrorCode GetDocPayloadWithErrorCode(docpayload_t& docPayload)
    {
        try
        {
            docPayload = GetDocPayload();
            return common::ErrorCode::OK;
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }

    common::ErrorCode GetTermPostingInfoWithErrorCode(index::TermPostingInfo& termPostingInfo) const
    {
        try
        {
            termPostingInfo = GetTermPostingInfo();
            return common::ErrorCode::OK;
        }
        catch (const misc::FileIOException& e)
        {
            return common::ErrorCode::FileIO;
        }
    }

};

DEFINE_SHARED_PTR(PostingIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXENGINEPOSTING_ITERATOR_H
