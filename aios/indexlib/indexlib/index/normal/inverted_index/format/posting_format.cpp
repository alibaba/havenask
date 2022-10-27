#include "indexlib/index/normal/inverted_index/format/posting_format.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingFormat);

PostingFormat::PostingFormat(const PostingFormatOption& postingFormatOption)
    : mDocListFormat(NULL)
    , mPositionListFormat(NULL)
{
    mDocListFormat = new DocListFormat(postingFormatOption.GetDocListFormatOption());
    if (postingFormatOption.HasPositionList())
    {
        mPositionListFormat = new PositionListFormat(
                postingFormatOption.GetPosListFormatOption());
    }
}

PostingFormat::~PostingFormat() 
{
    DELETE_AND_SET_NULL(mDocListFormat);
    DELETE_AND_SET_NULL(mPositionListFormat);
}

IE_NAMESPACE_END(index);

