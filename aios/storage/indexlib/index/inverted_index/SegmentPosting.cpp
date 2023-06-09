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
#include "indexlib/index/inverted_index/SegmentPosting.h"

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/inverted_index/PostingWriter.h"
#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SegmentPosting);

void SegmentPosting::Init(uint8_t compressMode, const std::shared_ptr<util::ByteSliceList>& sliceList,
                          docid_t baseDocId, uint64_t docCount)
{
    _sliceList = sliceList;
    _baseDocId = baseDocId;
    _docCount = docCount;
    _compressMode = compressMode;
    _isSingleSlice = false;
    _mainChainTermMeta = GetCurrentTermMeta();
    _postingWriter = NULL;
    _resource = nullptr;
}

void SegmentPosting::Init(docid_t baseDocId, uint64_t docCount)
{
    _baseDocId = baseDocId;
    _docCount = docCount;
}

void SegmentPosting::Init(uint8_t* data, size_t dataLen, docid_t baseDocId, uint64_t docCount, dictvalue_t dictValue)
{
    assert(data != nullptr);
    _isSingleSlice = true;
    util::ByteSlice slice;
    slice.data = data;
    slice.size = dataLen;
    _singleSlice = slice;

    _baseDocId = baseDocId;
    _docCount = docCount;
    _compressMode = ShortListOptimizeUtil::GetCompressMode(dictValue);
    _mainChainTermMeta = GetCurrentTermMeta();
    _postingWriter = NULL;
    _resource = nullptr;
}

void SegmentPosting::Init(docid_t baseDocId, uint64_t docCount, dictvalue_t dictValue, bool isDocList, bool dfFirst)
{
    _baseDocId = baseDocId;
    _docCount = docCount;
    _compressMode = ShortListOptimizeUtil::GetCompressMode(dictValue);
    _dictInlinePostingData = ShortListOptimizeUtil::GetDictInlineValue(dictValue);
    _isDocListDictInline = isDocList;
    _dfFirstDictInline = dfFirst;
    _isSingleSlice = false;
    _mainChainTermMeta = GetCurrentTermMeta();
    _postingWriter = NULL;
    _resource = nullptr;
}

void SegmentPosting::Init(docid_t baseDocId, uint32_t docCount, PostingWriter* postingWriter)
{
    _baseDocId = baseDocId;
    _docCount = docCount;
    _isSingleSlice = false;
    _postingWriter = postingWriter;
    GetTermMetaForRealtime(_mainChainTermMeta);
}

TermMeta SegmentPosting::GetCurrentTermMeta() const
{
    if (_isSingleSlice) {
        TermMeta tm;
        TermMetaLoader tmLoader(_postingFormatOption);
        file_system::ByteSliceReader reader;
        reader.Open(const_cast<util::ByteSlice*>(&_singleSlice));
        tmLoader.Load(&reader, tm);
        return tm;
    }

    if (ShortListOptimizeUtil::GetDocCompressMode(_compressMode) == DICT_INLINE_COMPRESS_MODE) {
        if (_isDocListDictInline) { // doclist dictinline
            docid_t beginDocid;
            df_t df;
            assert(_dfFirstDictInline != std::nullopt);
            DictInlineDecoder::DecodeContinuousDocId({_dfFirstDictInline.value(), _dictInlinePostingData}, beginDocid,
                                                     df);
            return TermMeta(df, df, 0);
        }
        // single doc doc inline
        DictInlineFormatter formatter(_postingFormatOption, _dictInlinePostingData);
        return TermMeta(formatter.GetDocFreq(),
                        formatter.GetTermFreq(), // df = 1, ttf = tf
                        formatter.GetTermPayload());
    }

    if (_postingWriter) {
        // in memory segment no truncate posting list
        return _mainChainTermMeta;
    }

    if (_sliceList) {
        TermMeta tm;
        TermMetaLoader tmLoader(_postingFormatOption);
        file_system::ByteSliceReader reader(_sliceList.get());
        tmLoader.Load(&reader, tm);
        return tm;
    }
    return _mainChainTermMeta;
}

bool SegmentPosting::operator==(const SegmentPosting& segPosting) const
{
    return (_sliceList == segPosting._sliceList) && (_mainChainTermMeta == segPosting._mainChainTermMeta) &&
           (_postingFormatOption == segPosting._postingFormatOption) && (_baseDocId == segPosting._baseDocId) &&
           (_docCount == segPosting._docCount) && (_compressMode == segPosting._compressMode) &&
           (_isSingleSlice == segPosting._isSingleSlice) && (_singleSlice == segPosting._singleSlice) &&
           (_postingWriter == segPosting._postingWriter);
}

void SegmentPosting::GetTermMetaForRealtime(TermMeta& tm)
{
    assert(_postingWriter);
    df_t df = _postingWriter->GetDF();
    tf_t ttf = _postingWriter->GetTotalTF();
    termpayload_t termPayload = _postingWriter->GetTermPayload();

    tm.SetDocFreq(df);
    tm.SetTotalTermFreq(ttf);
    tm.SetPayload(termPayload);
}
} // namespace indexlib::index
