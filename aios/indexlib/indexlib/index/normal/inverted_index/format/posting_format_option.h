#ifndef __INDEXLIB_POSTING_FORMAT_OPTION_H
#define __INDEXLIB_POSTING_FORMAT_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"
#include "indexlib/index_define.h"
#include "indexlib/index/normal/inverted_index/format/position_list_format_option.h"
#include "indexlib/config/index_config.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(index);

class PostingFormatOption
{
public:
    PostingFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL);

public:
    void Init(const config::IndexConfigPtr& indexConfigPtr);
    bool HasTfBitmap() const { return mDocListFormatOption.HasTfBitmap(); }
    bool HasTfList() const { return mDocListFormatOption.HasTfList(); }
    bool HasFieldMap() const { return mDocListFormatOption.HasFieldMap(); }
    bool HasDocPayload() const { return mDocListFormatOption.HasDocPayload(); }
    bool HasPositionList() const { return mPosListFormatOption.HasPositionList(); }
    bool HasPositionPayload() const { return mPosListFormatOption.HasPositionPayload(); }
    bool HasTermFrequency() const { return  mDocListFormatOption.HasTermFrequency(); }
    bool HasTermPayload() const { return mHasTermPayload; }

    const DocListFormatOption& GetDocListFormatOption() const
    { return mDocListFormatOption; }

    const PositionListFormatOption& GetPosListFormatOption() const
    { return mPosListFormatOption; }

    bool operator == (const PostingFormatOption& right) const;
    bool IsDictInlineCompress() const
    { return mDictInlineItemCount > 0; }

    uint8_t GetDictInlineCompressItemCount() const
    { return mDictInlineItemCount; }

    index::CompressMode GetDocListCompressMode() const 
    { return mDocListCompressMode; }
    bool IsReferenceCompress() const 
    { return IsReferenceCompress(mDocListCompressMode); }
    static bool IsReferenceCompress(index::CompressMode mode)
    { return mode == index::REFERENCE_COMPRESS_MODE; }

    PostingFormatOption GetBitmapPostingFormatOption() const;

    bool IsCompressedPostingHeader() const
    { return mIsCompressedPostingHeader; }

    void SetIsCompressedPostingHeader(bool isCompressed)
    { mIsCompressedPostingHeader = isCompressed; }
    
private:
    void InitOptionFlag(optionflag_t optionFlag);
private:
    bool mHasTermPayload;
    DocListFormatOption mDocListFormatOption;
    PositionListFormatOption mPosListFormatOption;
    uint8_t mDictInlineItemCount;
    bool mIsCompressedPostingHeader;
    index::CompressMode mDocListCompressMode;

private:
    friend class JsonizablePostingFormatOption;
    friend class PostingFormatOptionTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PostingFormatOption);

class JsonizablePostingFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizablePostingFormatOption(PostingFormatOption option = PostingFormatOption());

    PostingFormatOption GetPostingFormatOption();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    bool mHasTermPayload;
    JsonizableDocListFormatOption mDocListFormatOption;
    JsonizablePositionListFormatOption mPosListFormatOption;
    uint8_t mDictInlineItemCount;
    bool mIsCompressedPostingHeader;
    index::CompressMode mDocListCompressMode;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POSTING_FORMAT_OPTION_H
