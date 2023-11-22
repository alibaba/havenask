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

#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class DictInlineFormatter
{
public:
    DictInlineFormatter(const PostingFormatOption& postingFormatOption);
    DictInlineFormatter(const PostingFormatOption& postingFormatOption, uint64_t dictValue);

    // for test
    DictInlineFormatter(const PostingFormatOption& postingFormatOption, const std::vector<uint32_t>& vec);

    ~DictInlineFormatter() {}

    void Init(uint64_t dictValue);
    void SetTermPayload(termpayload_t termPayload) { _termPayload = termPayload; }

    void SetDocId(docid32_t docid) { _docId = docid; }

    void SetDocPayload(docpayload_t docPayload) { _docPayload = docPayload; }

    void SetTermFreq(tf_t termFreq) { _termFreq = termFreq; }

    void SetFieldMap(fieldmap_t fieldMap) { _fieldMap = fieldMap; }

    termpayload_t GetTermPayload() const { return _termPayload; }

    docid32_t GetDocId() const { return _docId; }

    docpayload_t GetDocPayload() const { return _docPayload; }

    df_t GetDocFreq() const { return 1; }
    tf_t GetTermFreq() const { return (_termFreq == INVALID_TF) ? 1 : _termFreq; }

    fieldmap_t GetFieldMap() const { return _fieldMap; }

    bool GetDictInlineValue(uint64_t& inlinePostingValue);

    static uint8_t CalculateDictInlineItemCount(PostingFormatOption& postingFormatOption);

private:
    void ToVector(std::vector<uint32_t>& vec);
    void FromVector(uint32_t* buffer, size_t size);
    uint32_t CheckAndGetValue(const size_t& cursor, uint32_t* buffer, size_t size);

    PostingFormatOption _postingFormatOption;
    termpayload_t _termPayload;
    docid32_t _docId;
    docpayload_t _docPayload;
    tf_t _termFreq;
    fieldmap_t _fieldMap;

    // TODO: add position list param in future
    friend class DictInlineFormatterTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
