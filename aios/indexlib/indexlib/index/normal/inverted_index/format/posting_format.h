#ifndef __INDEXLIB_POSTING_FORMAT_H
#define __INDEXLIB_POSTING_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format.h"

IE_NAMESPACE_BEGIN(index);

class PostingFormat
{
public:
    PostingFormat(const PostingFormatOption& postingFormatOption);
    ~PostingFormat();

public:
    DocListFormat* GetDocListFormat() const
    { return mDocListFormat; }

    PositionListFormat* GetPositionListFormat() const
    { return mPositionListFormat; }

private:
    DocListFormat* mDocListFormat;
    PositionListFormat* mPositionListFormat;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingFormat);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_FORMAT_H
