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
#include "indexlib/index/inverted_index/format/TermMetaDumper.h"

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"

namespace indexlib::index {

TermMetaDumper::TermMetaDumper(const PostingFormatOption& option) : _option(option) {}

uint32_t TermMetaDumper::CalculateStoreSize(const TermMeta& termMeta) const
{
    uint32_t len = VByteCompressor::GetVInt32Length(termMeta.GetDocFreq());
    bool isCompressed = _option.IsCompressedPostingHeader();
    if (!isCompressed || _option.HasTermFrequency()) {
        len += VByteCompressor::GetVInt32Length(termMeta.GetTotalTermFreq());
    }
    if (!isCompressed || _option.HasTermPayload()) {
        len += sizeof(termpayload_t);
    }
    return len;
}

void TermMetaDumper::Dump(const std::shared_ptr<file_system::FileWriter>& file, const TermMeta& termMeta) const
{
    assert(file != nullptr);

    file->WriteVUInt32(termMeta.GetDocFreq()).GetOrThrow();
    bool isCompressed = _option.IsCompressedPostingHeader();
    if (!isCompressed || _option.HasTermFrequency()) {
        file->WriteVUInt32(termMeta.GetTotalTermFreq()).GetOrThrow();
    }
    if (!isCompressed || _option.HasTermPayload()) {
        termpayload_t payload = termMeta.GetPayload();
        file->Write((void*)(&payload), sizeof(payload)).GetOrThrow();
    }
}
} // namespace indexlib::index
