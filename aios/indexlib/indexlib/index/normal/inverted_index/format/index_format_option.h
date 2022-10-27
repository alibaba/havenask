#ifndef __INDEXLIB_INDEX_FORMAT_OPTION_H
#define __INDEXLIB_INDEX_FORMAT_OPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"
#include "autil/legacy/jsonizable.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

class IndexFormatOption
{
public:
    IndexFormatOption();
    ~IndexFormatOption();
public:
    void Init(const config::IndexConfigPtr& indexConfigPtr);

    bool HasSectionAttribute() const { return mHasSectionAttribute; }
    bool HasBitmapIndex() const { return mHasBitmapIndex; }

    bool HasTermPayload() const { return mPostingFormatOption.HasTermPayload(); }
    bool HasDocPayload() const { return mPostingFormatOption.HasDocPayload(); }
    bool HasPositionPayload() const { return mPostingFormatOption.HasPositionPayload(); }

    bool IsNumberIndex() const { return mIsNumberIndex; }
    void SetNumberIndex(bool isNumberIndex) { mIsNumberIndex = isNumberIndex; }

    bool Load(const file_system::DirectoryPtr& directory);
    void Store(const file_system::DirectoryPtr& directory);
    
    const PostingFormatOption& GetPostingFormatOption() const
    { return mPostingFormatOption; }

    void SetIsCompressedPostingHeader(bool isCompressed)
    { mPostingFormatOption.SetIsCompressedPostingHeader(isCompressed); }

    bool IsReferenceCompress() const
    { return mPostingFormatOption.IsReferenceCompress(); }

public:
    static std::string ToString(const IndexFormatOption& option);
    static IndexFormatOption FromString(const std::string& str);
    static bool IsNumberIndex(const config::IndexConfigPtr& indexConfigPtr);

private:
    bool mHasSectionAttribute;
    bool mHasBitmapIndex;
    bool mIsNumberIndex;
    PostingFormatOption mPostingFormatOption;
private:
    friend class JsonizableIndexFormatOption;
    IE_LOG_DECLARE();
};

class JsonizableIndexFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizableIndexFormatOption(IndexFormatOption option = IndexFormatOption());

    const IndexFormatOption& GetIndexFormatOption() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

private:
    IndexFormatOption mOption;
    JsonizablePostingFormatOption mPostingFormatOption;
};

DEFINE_SHARED_PTR(IndexFormatOption);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_FORMAT_OPTION_H
