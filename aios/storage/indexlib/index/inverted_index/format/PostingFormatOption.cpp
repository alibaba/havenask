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
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PostingFormatOption);

bool PostingFormatOption::operator==(const PostingFormatOption& right) const
{
    return _hasTermPayload == right._hasTermPayload && _docListFormatOption == right._docListFormatOption &&
           _posListFormatOption == right._posListFormatOption && _dictInlineItemCount == right._dictInlineItemCount &&
           _isCompressedPostingHeader == right._isCompressedPostingHeader && _formatVersion == right._formatVersion &&
           _docListCompressMode == right._docListCompressMode;
}

PostingFormatOption PostingFormatOption::GetBitmapPostingFormatOption() const
{
    optionflag_t bitmapOptionFlag = 0;
    if (HasTermPayload()) {
        bitmapOptionFlag |= of_term_payload;
    }
    PostingFormatOption option(bitmapOptionFlag);
    option.SetIsCompressedPostingHeader(false);
    return option;
}

JsonizablePostingFormatOption::JsonizablePostingFormatOption(PostingFormatOption option)
    : _hasTermPayload(option._hasTermPayload)
    , _docListFormatOption(option._docListFormatOption)
    , _posListFormatOption(option._posListFormatOption)
    , _dictInlineItemCount(option._dictInlineItemCount)
    , _isCompressedPostingHeader(option._isCompressedPostingHeader)
    , _formatVersion(option._formatVersion)
    , _docListCompressMode(option._docListCompressMode)
{
}

PostingFormatOption JsonizablePostingFormatOption::GetPostingFormatOption()
{
    PostingFormatOption option;
    option._hasTermPayload = _hasTermPayload;
    option._docListFormatOption = _docListFormatOption.GetDocListFormatOption();
    option._posListFormatOption = _posListFormatOption.GetPositionListFormatOption();
    option._dictInlineItemCount = _dictInlineItemCount;
    option._isCompressedPostingHeader = _isCompressedPostingHeader;
    option._docListCompressMode = _docListCompressMode;
    option._formatVersion = _formatVersion;
    return option;
}

void JsonizablePostingFormatOption::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("has_term_payload", _hasTermPayload);
    json.Jsonize("doc_list_format_option", _docListFormatOption);
    json.Jsonize("pos_list_format_option", _posListFormatOption);
    json.Jsonize("dict_inline_item_count", _dictInlineItemCount);
    json.Jsonize("is_compressed_posting_header", _isCompressedPostingHeader);
    json.Jsonize("format_version_id", _formatVersion, _formatVersion);
    json.Jsonize("doc_list_compress_mode", _docListCompressMode, _docListCompressMode);
}

} // namespace indexlib::index
