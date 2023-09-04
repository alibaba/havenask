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

#include <stddef.h>
#include <stdint.h>

#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/util/FieldBoostTable.h"

namespace indexlib {
namespace index {
class InDocPositionState;
} // namespace index
} // namespace indexlib

namespace suez {
namespace turing {

class TermMatchData {
public:
    TermMatchData() {}
    TermMatchData(indexlib::index::TermMatchData &tmd) { _tmd = tmd; }
    TermMatchData(const TermMatchData &tmd) = delete;
    ~TermMatchData() {
        if (NULL != _tmd.GetInDocPositionState()) {
            _tmd.FreeInDocPositionState();
        }
    }

private:
    TermMatchData &operator=(const TermMatchData &);

public:
    inline bool isMatched() const { return _tmd.IsMatched(); }
    inline tf_t getTermFreq() const { return _tmd.GetTermFreq(); }
    inline uint32_t getFirstOcc() const { return _tmd.GetFirstOcc(); }
    /* return the matched 'fieldbitmap' for this term. */
    inline fieldbitmap_t getFieldBitmap() const { return _tmd.GetFieldMap(); }
    inline std::shared_ptr<indexlib::index::InDocPositionIterator> getInDocPositionIterator() const {
        return _tmd.GetInDocPositionIterator();
    }
    inline indexlib::index::TermMatchData &getTermMatchData() { return _tmd; }
    inline const indexlib::index::TermMatchData &getTermMatchData() const { return _tmd; }

    inline docpayload_t getDocPayload() const { return _tmd.GetDocPayload(); }

    inline void setDocPayload(docpayload_t docPayload) { _tmd.SetDocPayload(docPayload); }
    inline void setTermFreq(tf_t termFreq) { _tmd.SetTermFreq(termFreq); }
    inline void setFirstOcc(uint32_t firstOcc) { _tmd.SetFirstOcc(firstOcc); }
    inline void setFieldBitmap(fieldbitmap_t fieldBitmap) { _tmd.SetFieldMap(fieldBitmap); }

    inline void setInDocPositionState(indexlib::index::InDocPositionState *state) { _tmd.SetInDocPositionState(state); }

    inline void setTermMatchData(const indexlib::index::TermMatchData &tmd) { _tmd = tmd; }

    // for test
    inline void setMatched(bool flag) { _tmd.SetMatched(flag); }

private:
    indexlib::index::TermMatchData _tmd;
};

} // namespace turing
} // namespace suez
