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

#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/index/inverted_index/InDocPositionState.h"
#include "indexlib/index/inverted_index/Types.h"

namespace indexlib { namespace index {

class TermMatchData
{
public:
    struct IndexInfo {
        uint8_t hasDocPayload : 1;
        uint8_t hasFieldMap   : 1;
        uint8_t hasFirstOcc   : 1;
        uint8_t isMatched     : 1;
        uint8_t unused        : 4;

        IndexInfo() { *((uint8_t*)this) = 0; }

        inline bool HasDocPayload() const { return hasDocPayload == 1; }

        inline void SetDocPayload(bool flag) { hasDocPayload = flag ? 1 : 0; }

        inline bool HasFieldMap() const { return hasFieldMap == 1; }

        inline void SetFieldMap(bool flag) { hasFieldMap = flag ? 1 : 0; }

        inline bool HasFirstOcc() const { return hasFirstOcc == 1; }

        inline void SetFirstOcc(bool flag) { hasFirstOcc = flag ? 1 : 0; }

        inline bool IsMatched() const { return isMatched == 1; }

        inline void SetMatched(bool flag) { isMatched = flag ? 1 : 0; }
    };

    TermMatchData() : _posState(nullptr), _firstOcc(0), _docPayload(0), _fieldMap(0) {}
    ~TermMatchData() {}

    tf_t GetTermFreq() const { return _posState != nullptr ? _posState->GetTermFreq() : 1; }

    pos_t GetFirstOcc() const { return _firstOcc; }

    fieldmap_t GetFieldMap() const { return _fieldMap; }

    docpayload_t GetDocPayload() const
    {
        if (!_indexInfo.HasDocPayload()) {
            return 0;
        }
        return _docPayload;
    }

    InDocPositionState* GetInDocPositionState() const { return _posState; }

    void SetInDocPositionState(InDocPositionState* state)
    {
        _posState = state;
        SetMatched(_posState != nullptr);
    }

    void FreeInDocPositionState()
    {
        if (_posState) {
            _posState->Free();
            _posState = nullptr;
        }
    }

    std::shared_ptr<InDocPositionIterator> GetInDocPositionIterator() const
    {
        if (_posState) {
            return std::shared_ptr<InDocPositionIterator>(_posState->CreateInDocIterator());
        }
        return nullptr;
    }

    void SetTermFreq(tf_t termFreq) { _posState->SetTermFreq(termFreq); }

    void SetFirstOcc(pos_t firstOcc)
    {
        _firstOcc = firstOcc;
        _indexInfo.SetFirstOcc(true);
    }

    void SetFieldMap(fieldmap_t fieldMap)
    {
        _fieldMap = fieldMap;
        _indexInfo.SetFieldMap(true);
    }

    bool HasFieldMap() const { return _indexInfo.HasFieldMap(); }

    bool HasFirstOcc() const { return _indexInfo.HasFirstOcc(); }

    void SetDocPayload(docpayload_t docPayload)
    {
        _docPayload = docPayload;
        _indexInfo.SetDocPayload(true);
    }

    void SetHasDocPayload(bool hasDocPayload) { _indexInfo.SetDocPayload(hasDocPayload); }

    bool HasDocPayload() const { return _indexInfo.HasDocPayload(); }

    bool IsMatched() const { return _indexInfo.IsMatched(); }

    // for test
    void SetMatched(bool flag) { _indexInfo.SetMatched(flag); }

private:
    InDocPositionState* _posState; // 8 bytes
    pos_t _firstOcc;               // 4 bytes
    docpayload_t _docPayload;      // 2 bytes
    fieldmap_t _fieldMap;          // 1 byte
    IndexInfo _indexInfo;          // 1 byte
};

}} // namespace indexlib::index
