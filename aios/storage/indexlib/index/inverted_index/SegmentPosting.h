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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib::index {
class PostingWriter;

class SegmentPosting
{
public:
    inline SegmentPosting(const PostingFormatOption& postingFormatOption = OPTION_FLAG_ALL)
        : _baseDocId(INVALID_DOCID)
        , _docCount(0)
        , _compressMode(0)
        , _isSingleSlice(false)
        , _isDocListDictInline(false)
        , _mainChainTermMeta()
        , _postingWriter(nullptr)
        , _resource(nullptr)
        , _dictInlinePostingData(INVALID_DICT_VALUE)
        , _postingFormatOption(postingFormatOption)
    {
    }

    ~SegmentPosting() {}

public:
    void Init(uint8_t compressMode, const std::shared_ptr<util::ByteSliceList>& sliceList, docid64_t baseDocId,
              uint64_t docCount);

    void Init(docid64_t baseDocId, uint64_t docCount);

    // for integrate memory optimize : use single slice, avoid new object
    void Init(uint8_t* data, size_t dataLen, docid64_t baseDocId, uint64_t docCount, dictvalue_t dictValue);

    // for dict inline compress
    void Init(docid64_t baseDocId, uint64_t docCount, dictvalue_t dictValue, bool isDocList, bool dfFirst);

    // for realtime segment posting
    void Init(docid64_t baseDocId, uint32_t docCount, PostingWriter* postingWriter);

    const std::shared_ptr<util::ByteSliceList>& GetSliceListPtr() const { return _sliceList; }

    util::ByteSlice* GetSingleSlice() const
    {
        return _isSingleSlice ? const_cast<util::ByteSlice*>(&_singleSlice) : NULL;
    }

    TermMeta GetCurrentTermMeta() const;

    void SetMainChainTermMeta(const TermMeta& termMeta) { _mainChainTermMeta = termMeta; }
    const TermMeta& GetMainChainTermMeta() const { return _mainChainTermMeta; }

    docid64_t GetBaseDocId() const { return _baseDocId; }
    void SetBaseDocId(docid64_t baseDocId) { _baseDocId = baseDocId; }

    uint32_t GetDocCount() const { return _docCount; }
    void SetDocCount(const uint32_t docCount) { _docCount = docCount; }

    uint8_t GetCompressMode() const { return _compressMode; }
    int64_t GetDictInlinePostingData() const { return _dictInlinePostingData; }

    const PostingWriter* GetInMemPostingWriter() const { return _postingWriter; }

    bool IsRealTimeSegment() const { return _postingWriter != NULL; }
    bool operator==(const SegmentPosting& segPosting) const;

    const PostingFormatOption& GetPostingFormatOption() const { return _postingFormatOption; }

    void SetPostingFormatOption(const PostingFormatOption& option) { _postingFormatOption = option; }

    void SetResource(void* resource) { _resource = resource; }
    void* GetResource() const { return _resource; }

    bool IsDictInline() const
    {
        return ShortListOptimizeUtil::GetDocCompressMode(_compressMode) == DICT_INLINE_COMPRESS_MODE;
    }

    bool IsDocListDictInline() const { return _isDocListDictInline; }
    bool IsDfFirst() const
    {
        assert(_dfFirstDictInline != std::nullopt);
        return _dfFirstDictInline.value();
    }

    void Reset()
    {
        _baseDocId = INVALID_DOCID;
        _docCount = 0;
        _compressMode = 0;
        _isSingleSlice = false;
        _mainChainTermMeta.Reset();
        _singleSlice = util::ByteSlice();
        _postingWriter = NULL;
        _resource = nullptr;
        _dictInlinePostingData = INVALID_DICT_VALUE;
    }

private:
    void GetTermMetaForRealtime(TermMeta& tm);

private:
    std::shared_ptr<util::ByteSliceList> _sliceList;
    docid64_t _baseDocId;
    uint32_t _docCount;
    uint8_t _compressMode;
    bool _isSingleSlice;
    bool _isDocListDictInline;
    std::optional<bool> _dfFirstDictInline;
    TermMeta _mainChainTermMeta;
    util::ByteSlice _singleSlice;
    PostingWriter* _postingWriter;
    void* _resource;

    int64_t _dictInlinePostingData;
    PostingFormatOption _postingFormatOption;

private:
    friend class BufferedPostingIteratorTest;
    friend class SegmentPostingTest;
    AUTIL_LOG_DECLARE();
};

using SegmentPostingVector = std::vector<SegmentPosting>;
} // namespace indexlib::index
