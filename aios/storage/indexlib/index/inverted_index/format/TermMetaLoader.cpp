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
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {

TermMetaLoader::TermMetaLoader(const PostingFormatOption& option) : _option(option) {}
void TermMetaLoader::Load(file_system::ByteSliceReader* sliceReader, TermMeta& termMeta) const
{
    df_t df = (df_t)sliceReader->ReadVUInt32().GetOrThrow();
    termMeta.SetDocFreq(df);
    bool isCompressed = _option.IsCompressedPostingHeader();
    if (!isCompressed || _option.HasTermFrequency()) {
        termMeta.SetTotalTermFreq((tf_t)sliceReader->ReadVUInt32().GetOrThrow());
    } else {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || _option.HasTermPayload()) {
        termpayload_t payload;
        sliceReader->Read((void*)(&payload), sizeof(payload)).GetOrThrow();
        termMeta.SetPayload(payload);
    } else {
        termMeta.SetPayload(0);
    }
}

void TermMetaLoader::Load(const std::shared_ptr<file_system::FileReader>& reader, TermMeta& termMeta) const
{
    df_t df = (df_t)reader->ReadVUInt32().GetOrThrow();
    termMeta.SetDocFreq(df);
    bool isCompressed = _option.IsCompressedPostingHeader();
    if (!isCompressed || _option.HasTermFrequency()) {
        termMeta.SetTotalTermFreq((tf_t)reader->ReadVUInt32().GetOrThrow());
    } else {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || _option.HasTermPayload()) {
        termpayload_t payload;
        reader->Read((void*)(&payload), sizeof(payload)).GetOrThrow();
        termMeta.SetPayload(payload);
    } else {
        termMeta.SetPayload(0);
    }
}

void TermMetaLoader::Load(uint8_t*& dataCursor, size_t& leftSize, TermMeta& termMeta) const
{
    auto [status, df] = VByteCompressor::DecodeVInt32(dataCursor, (uint32_t&)leftSize);
    THROW_IF_STATUS_ERROR(status);
    termMeta.SetDocFreq(df);
    bool isCompressed = _option.IsCompressedPostingHeader();
    if (!isCompressed || _option.HasTermFrequency()) {
        auto [status, ttf] = VByteCompressor::DecodeVInt32(dataCursor, (uint32_t&)leftSize);
        THROW_IF_STATUS_ERROR(status);
        termMeta.SetTotalTermFreq(ttf);
    } else {
        termMeta.SetTotalTermFreq(df);
    }
    if (!isCompressed || _option.HasTermPayload()) {
        termpayload_t payload = *(termpayload_t*)dataCursor;
        dataCursor += sizeof(termpayload_t);
        leftSize -= sizeof(termpayload_t);

        termMeta.SetPayload(payload);
    } else {
        termMeta.SetPayload(0);
    }
}
} // namespace indexlib::index
