#ifndef __INDEXLIB_IN_MEM_POSTING_DECODER_H
#define __INDEXLIB_IN_MEM_POSTING_DECODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_doc_list_decoder.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_position_list_decoder.h"

IE_NAMESPACE_BEGIN(index);

class InMemPostingDecoder
{
public:
    InMemPostingDecoder();
    ~InMemPostingDecoder();

public:
    void SetDocListDecoder(InMemDocListDecoder* docListDecoder)
    { mDocListDecoder = docListDecoder; }

    InMemDocListDecoder* GetInMemDocListDecoder() const
    { return mDocListDecoder; }

    void SetPositionListDecoder(InMemPositionListDecoder* positionListDecoder)
    { mPositionListDecoder = positionListDecoder; }

    InMemPositionListDecoder* GetInMemPositionListDecoder() const
    { return mPositionListDecoder; }

private:
    InMemDocListDecoder* mDocListDecoder;
    InMemPositionListDecoder* mPositionListDecoder;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemPostingDecoder);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_POSTING_DECODER_H
