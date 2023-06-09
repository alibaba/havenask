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
#include "indexlib/index/inverted_index/format/DictInlineFormatter.h"

#include "indexlib/base/Constant.h"
#include "indexlib/index/inverted_index/format/DictInlineDecoder.h"
#include "indexlib/index/inverted_index/format/DictInlineEncoder.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, DictInlineFormatter);

DictInlineFormatter::DictInlineFormatter(const PostingFormatOption& postingFormatOption)
    : _postingFormatOption(postingFormatOption)
    , _termPayload(0)
    , _docId(INVALID_DOCID)
    , _docPayload(INVALID_DOC_PAYLOAD)
    , _termFreq(INVALID_TF)
    , _fieldMap(0)
{
}

DictInlineFormatter::DictInlineFormatter(const PostingFormatOption& postingFormatOption, uint64_t dictValue)
    : DictInlineFormatter(postingFormatOption)
{
    Init(dictValue);
}

DictInlineFormatter::DictInlineFormatter(const PostingFormatOption& postingFormatOption,
                                         const std::vector<uint32_t>& vec)
    : _postingFormatOption(postingFormatOption)
{
    FromVector((uint32_t*)vec.data(), vec.size());
}

void DictInlineFormatter::Init(uint64_t dictValue)
{
    uint32_t dictItemCount = _postingFormatOption.GetDictInlineCompressItemCount();
    assert(dictItemCount > 0);

    uint32_t vec[dictItemCount];
    DictInlineDecoder::Decode(dictValue, dictItemCount, vec);
    FromVector(vec, dictItemCount);
}

bool DictInlineFormatter::GetDictInlineValue(uint64_t& inlineDictValue)
{
    std::vector<uint32_t> vec;
    ToVector(vec);
    return DictInlineEncoder::Encode(vec, inlineDictValue);
}

void DictInlineFormatter::ToVector(std::vector<uint32_t>& vec)
{
    assert(vec.empty());

    if (_postingFormatOption.HasTermPayload()) {
        vec.push_back(_termPayload);
    }

    vec.push_back(_docId);

    if (_postingFormatOption.HasDocPayload()) {
        vec.push_back(_docPayload);
    }

    if (_postingFormatOption.HasTfList()) {
        vec.push_back(_termFreq);
    }

    if (_postingFormatOption.HasFieldMap()) {
        vec.push_back(_fieldMap);
    }
}

void DictInlineFormatter::FromVector(uint32_t* buffer, size_t size)
{
    size_t cursor = 0;

    if (_postingFormatOption.HasTermPayload()) {
        _termPayload = (termpayload_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    _docId = (docid_t)CheckAndGetValue(cursor, buffer, size);
    cursor++;

    if (_postingFormatOption.HasDocPayload()) {
        _docPayload = (docpayload_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (_postingFormatOption.HasTfList()) {
        _termFreq = (tf_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (_postingFormatOption.HasFieldMap()) {
        _fieldMap = (fieldmap_t)CheckAndGetValue(cursor, buffer, size);
        cursor++;
    }

    if (cursor < size) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "DictInLineMapper FromVector failed: "
                       "expect vector elements num is %ld,"
                       "vecotr size is %ld.",
                       cursor, size);
    }
}

uint32_t DictInlineFormatter::CheckAndGetValue(const size_t& cursor, uint32_t* buffer, size_t size)
{
    if (cursor >= size) {
        INDEXLIB_THROW(indexlib::util::OutOfRangeException,
                       "DictInLineMapper get vector value failed: "
                       "expect vector elements idx is %ld,"
                       "vector size is %ld",
                       cursor, size);
    }

    return buffer[cursor];
}

uint8_t DictInlineFormatter::CalculateDictInlineItemCount(PostingFormatOption& postingFormatOption)
{
    return postingFormatOption.CalculateDictInlineItemCount();
}

} // namespace indexlib::index
