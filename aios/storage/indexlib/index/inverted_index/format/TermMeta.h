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

#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {

class TermMeta
{
public:
    TermMeta() : _docFreq(0), _totalTermFreq(0), _payload(0) {}

    TermMeta(df_t docFreq, tf_t totalTermFreq, termpayload_t payload = 0)
        : _docFreq(docFreq)
        , _totalTermFreq(totalTermFreq)
        , _payload(payload)
    {
    }

    TermMeta(const TermMeta& termMeta)
        : _docFreq(termMeta._docFreq)
        , _totalTermFreq(termMeta._totalTermFreq)
        , _payload(termMeta._payload)
    {
    }

    df_t GetDocFreq() const { return _docFreq; }
    tf_t GetTotalTermFreq() const { return _totalTermFreq; }
    termpayload_t GetPayload() const { return _payload; }

    void SetPayload(termpayload_t payload) { _payload = payload; }
    void SetDocFreq(df_t docFreq) { _docFreq = docFreq; }
    void SetTotalTermFreq(tf_t totalTermFreq) { _totalTermFreq = totalTermFreq; }

    TermMeta& operator=(const TermMeta& payload);
    bool operator==(const TermMeta& payload) const;

    void Reset()
    {
        _docFreq = 0;
        _totalTermFreq = 0;
        _payload = 0;
    }

private:
    df_t _docFreq;
    tf_t _totalTermFreq;
    termpayload_t _payload;
};

} // namespace indexlib::index
