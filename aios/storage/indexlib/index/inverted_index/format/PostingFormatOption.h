/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/DocListFormatOption.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"

namespace indexlib::index {

class PostingFormatOption
{
public:
    inline PostingFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL)
        : _hasTermPayload(false)
        , _dictInlineItemCount(0)
        , _isCompressedPostingHeader(true)
        , _formatVersion(0)
        , _docListCompressMode(indexlib::index::PFOR_DELTA_COMPRESS_MODE)
    {
        InitOptionFlag(optionFlag);
        _dictInlineItemCount = CalculateDictInlineItemCount();
    }
    ~PostingFormatOption() = default;

    inline void Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr)
    {
        InitOptionFlag(indexConfigPtr->GetOptionFlag());

        _dictInlineItemCount = CalculateDictInlineItemCount();
        _docListCompressMode = indexConfigPtr->IsReferenceCompress() ? indexlib::index::REFERENCE_COMPRESS_MODE
                                                                     : indexlib::index::PFOR_DELTA_COMPRESS_MODE;
        _formatVersion = indexConfigPtr->GetIndexFormatVersionId();
        _docListFormatOption.SetShortListVbyteCompress(indexConfigPtr->IsShortListVbyteCompress());
    }

    bool HasTfBitmap() const { return _docListFormatOption.HasTfBitmap(); }
    bool HasTfList() const { return _docListFormatOption.HasTfList(); }
    bool HasFieldMap() const { return _docListFormatOption.HasFieldMap(); }
    bool HasDocPayload() const { return _docListFormatOption.HasDocPayload(); }
    bool HasPositionList() const { return _posListFormatOption.HasPositionList(); }
    bool HasPositionPayload() const { return _posListFormatOption.HasPositionPayload(); }
    bool HasTermFrequency() const { return _docListFormatOption.HasTermFrequency(); }
    bool HasTermPayload() const { return _hasTermPayload; }
    bool IsShortListVbyteCompress() const { return _docListFormatOption.IsShortListVbyteCompress(); }
    format_versionid_t GetFormatVersion() const { return _formatVersion; }
    void SetFormatVersion(format_versionid_t id) { _formatVersion = id; }
    void SetShortListVbyteCompress(bool flag) { _docListFormatOption.SetShortListVbyteCompress(flag); }

    const DocListFormatOption& GetDocListFormatOption() const { return _docListFormatOption; }

    const PositionListFormatOption& GetPosListFormatOption() const { return _posListFormatOption; }

    bool operator==(const PostingFormatOption& right) const;
    uint8_t GetDictInlineCompressItemCount() const { return _dictInlineItemCount; }

    bool IsOnlyDocId() const
    {
        return _dictInlineItemCount == 1 && !HasTfBitmap() && !HasPositionPayload() && !HasPositionList();
    }

    bool IsOnlyTermPayLoad() const
    {
        return _dictInlineItemCount == 2 && HasTermPayload() && !HasTfBitmap() && !HasPositionPayload() &&
               !HasPositionList();
    }

    indexlib::index::CompressMode GetDocListCompressMode() const { return _docListCompressMode; }
    bool IsReferenceCompress() const { return IsReferenceCompress(_docListCompressMode); }
    static bool IsReferenceCompress(indexlib::index::CompressMode mode)
    {
        return mode == indexlib::index::REFERENCE_COMPRESS_MODE;
    }

    PostingFormatOption GetBitmapPostingFormatOption() const;

    bool IsCompressedPostingHeader() const { return _isCompressedPostingHeader; }

    void SetIsCompressedPostingHeader(bool isCompressed) { _isCompressedPostingHeader = isCompressed; }

    inline uint8_t CalculateDictInlineItemCount()
    {
        // docid always exist
        uint8_t itemCount = 1;
        if (HasTermPayload()) {
            ++itemCount;
        }

        if (HasDocPayload()) {
            ++itemCount;
        }

        if (HasTfList()) {
            ++itemCount;
        }

        if (HasFieldMap()) {
            ++itemCount;
        }
        return itemCount;
    }

private:
    inline void InitOptionFlag(optionflag_t optionFlag)
    {
        _hasTermPayload = optionFlag & of_term_payload;
        _docListFormatOption.Init(optionFlag);
        _posListFormatOption.Init(optionFlag);
    }

    bool _hasTermPayload;
    DocListFormatOption _docListFormatOption;
    PositionListFormatOption _posListFormatOption;
    uint8_t _dictInlineItemCount;
    bool _isCompressedPostingHeader;
    format_versionid_t _formatVersion;
    indexlib::index::CompressMode _docListCompressMode;

    friend class JsonizablePostingFormatOption;
    friend class PostingFormatOptionTest;

    AUTIL_LOG_DECLARE();
};

class JsonizablePostingFormatOption : public autil::legacy::Jsonizable
{
public:
    JsonizablePostingFormatOption(PostingFormatOption option = PostingFormatOption());

    PostingFormatOption GetPostingFormatOption();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    bool _hasTermPayload;
    JsonizableDocListFormatOption _docListFormatOption;
    JsonizablePositionListFormatOption _posListFormatOption;
    uint8_t _dictInlineItemCount;
    bool _isCompressedPostingHeader;
    format_versionid_t _formatVersion = 0;
    indexlib::index::CompressMode _docListCompressMode;
};

} // namespace indexlib::index
