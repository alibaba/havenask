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
#include "indexlib/file_system/Directory.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class IndexFormatOption
{
public:
    inline IndexFormatOption()
    {
        _hasSectionAttribute = false;
        _hasBitmapIndex = false;
        _isNumberIndex = false;
    }

    virtual ~IndexFormatOption() = default;

    void Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr);

    bool HasSectionAttribute() const { return _hasSectionAttribute; }
    bool HasBitmapIndex() const { return _hasBitmapIndex; }

    bool HasTermPayload() const { return _postingFormatOption.HasTermPayload(); }
    bool HasDocPayload() const { return _postingFormatOption.HasDocPayload(); }
    bool HasPositionPayload() const { return _postingFormatOption.HasPositionPayload(); }

    bool IsNumberIndex() const { return _isNumberIndex; }
    void SetNumberIndex(bool isNumberIndex) { _isNumberIndex = isNumberIndex; }

    bool Load(const std::shared_ptr<file_system::Directory>& directory);
    void Store(const std::shared_ptr<file_system::Directory>& directory);

    const PostingFormatOption& GetPostingFormatOption() const { return _postingFormatOption; }

    void SetIsCompressedPostingHeader(bool isCompressed)
    {
        _postingFormatOption.SetIsCompressedPostingHeader(isCompressed);
    }

    bool IsReferenceCompress() const { return _postingFormatOption.IsReferenceCompress(); }

    static std::string ToString(const IndexFormatOption& option);
    static IndexFormatOption FromString(const std::string& str);

    static bool TEST_Equals(IndexFormatOption* lhs, IndexFormatOption* rhs);

protected:
    virtual bool OwnSectionAttribute(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr);

protected:
    bool _hasSectionAttribute;
    bool _hasBitmapIndex;
    bool _isNumberIndex;
    PostingFormatOption _postingFormatOption;

    friend class JsonizableIndexFormatOption;

    AUTIL_LOG_DECLARE();
};

class JsonizableIndexFormatOption : public autil::legacy::Jsonizable
{
public:
    explicit JsonizableIndexFormatOption(IndexFormatOption option = IndexFormatOption());

    const IndexFormatOption& GetIndexFormatOption() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    IndexFormatOption _option;
    JsonizablePostingFormatOption _postingFormatOption;
};

} // namespace indexlib::index
