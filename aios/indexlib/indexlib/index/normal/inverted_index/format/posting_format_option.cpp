#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "indexlib/index/normal/inverted_index/format/dict_inline_formatter.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingFormatOption);

PostingFormatOption::PostingFormatOption(optionflag_t optionFlag) 
    : mDictInlineItemCount(0)
    , mIsCompressedPostingHeader(true)
    , mDocListCompressMode(index::PFOR_DELTA_COMPRESS_MODE)
{
    InitOptionFlag(optionFlag);
    mDictInlineItemCount = DictInlineFormatter::CalculateDictInlineItemCount(*this);
}

void PostingFormatOption::Init(const IndexConfigPtr& indexConfigPtr)
{
    InitOptionFlag(indexConfigPtr->GetOptionFlag());

    mDictInlineItemCount = DictInlineFormatter::CalculateDictInlineItemCount(*this);
    mDocListCompressMode = indexConfigPtr->IsReferenceCompress() ? 
                           index::REFERENCE_COMPRESS_MODE :
                           index::PFOR_DELTA_COMPRESS_MODE;
}

void PostingFormatOption::InitOptionFlag(optionflag_t optionFlag)
{
    mHasTermPayload = optionFlag & of_term_payload;
    mDocListFormatOption.Init(optionFlag);
    mPosListFormatOption.Init(optionFlag);
}

bool PostingFormatOption::operator == (const PostingFormatOption& right) const
{
    return mHasTermPayload == right.mHasTermPayload
        && mDocListFormatOption == right.mDocListFormatOption
        && mPosListFormatOption == right.mPosListFormatOption
        && mDictInlineItemCount == right.mDictInlineItemCount
        && mIsCompressedPostingHeader == right.mIsCompressedPostingHeader;
}

PostingFormatOption PostingFormatOption::GetBitmapPostingFormatOption() const
{
    optionflag_t bitmapOptionFlag = 0;
    if (HasTermPayload())
    {
        bitmapOptionFlag |= of_term_payload;
    }
    PostingFormatOption option(bitmapOptionFlag);
    option.SetIsCompressedPostingHeader(false);
    return option;
}

JsonizablePostingFormatOption::JsonizablePostingFormatOption(
        PostingFormatOption option)
    : mHasTermPayload(option.mHasTermPayload)
    , mDocListFormatOption(option.mDocListFormatOption)
    , mPosListFormatOption(option.mPosListFormatOption)
    , mDictInlineItemCount(option.mDictInlineItemCount)
    , mIsCompressedPostingHeader(option.mIsCompressedPostingHeader)
    , mDocListCompressMode(option.mDocListCompressMode)
{
}

PostingFormatOption JsonizablePostingFormatOption::GetPostingFormatOption()
{
    PostingFormatOption option;
    option.mHasTermPayload = mHasTermPayload;
    option.mDocListFormatOption = mDocListFormatOption.GetDocListFormatOption();
    option.mPosListFormatOption = mPosListFormatOption.GetPositionListFormatOption();
    option.mDictInlineItemCount = mDictInlineItemCount;
    option.mIsCompressedPostingHeader = mIsCompressedPostingHeader;
    option.mDocListCompressMode = mDocListCompressMode;
    return option;
}

void JsonizablePostingFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_term_payload", mHasTermPayload);
    json.Jsonize("doc_list_format_option", mDocListFormatOption);
    json.Jsonize("pos_list_format_option", mPosListFormatOption);
    json.Jsonize("dict_inline_item_count", mDictInlineItemCount);
    json.Jsonize("is_compressed_posting_header", mIsCompressedPostingHeader);
    json.Jsonize("doc_list_compress_mode", mDocListCompressMode, mDocListCompressMode);
}

IE_NAMESPACE_END(index);

