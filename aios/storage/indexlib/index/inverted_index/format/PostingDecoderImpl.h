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

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/common/numeric_compress/NewPfordeltaIntEncoder.h"
#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"
#include "indexlib/index/inverted_index/format/PostingDecoder.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/util/Bitmap.h"

namespace indexlib::index {

class PostingDecoderImpl : public PostingDecoder
{
public:
    PostingDecoderImpl(const PostingFormatOption& postingFormatOption);
    virtual ~PostingDecoderImpl();

public:
    void Init(TermMeta* termMeta, const std::shared_ptr<file_system::ByteSliceReader>& postingListReader,
              const std::shared_ptr<file_system::ByteSliceReader>& positionListReader,
              const std::shared_ptr<util::Bitmap>& tfBitmap, size_t postingDataLen);

    // init for dict inline compress
    void Init(TermMeta* termMeta, dictvalue_t dictValue, bool isDocList, bool dfFirst);

    // virtual for test
    virtual uint32_t DecodeDocList(docid_t* docIdBuf, tf_t* tfListBuf, docpayload_t* docPayloadBuf,
                                   fieldmap_t* fieldMapBuf, size_t len);

    uint32_t DecodePosList(pos_t* posListBuf, pospayload_t* posPayloadBuf, size_t len);

    bool IsDocEnd(uint32_t posIndex)
    {
        assert(_postingFormatOption.HasTfBitmap());
        if (posIndex >= _tfBitmap->GetValidItemCount()) {
            return true;
        }
        return _tfBitmap->Test(posIndex);
    }

    size_t GetPostingDataLength() const { return _postingDataLength; }

private:
    // virtual for test
    virtual void InitDocListEncoder(const DocListFormatOption& docListFormatOption, df_t df);
    virtual void InitPosListEncoder(const PositionListFormatOption& posListFormatOption, ttf_t totalTF);

    std::shared_ptr<file_system::ByteSliceReader> _postingListReader;
    std::shared_ptr<file_system::ByteSliceReader> _positionListReader;

    std::shared_ptr<util::Bitmap> _tfBitmap;
    dictvalue_t _inlineDictValue;
    bool _isDocListDictInline;
    std::optional<bool> _dfFirstDictInline;

    const Int32Encoder* _docIdEncoder;
    const Int32Encoder* _tfListEncoder;
    const Int16Encoder* _docPayloadEncoder;
    const Int8Encoder* _fieldMapEncoder;
    const Int32Encoder* _positionEncoder;
    const Int8Encoder* _posPayloadEncoder;

    df_t _decodedDocCount;
    tf_t _decodedPosCount;
    size_t _postingDataLength;
    PostingFormatOption _postingFormatOption;

private:
    friend class PostingDecoderImplTest;
    friend class OneDocMergerTest;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
