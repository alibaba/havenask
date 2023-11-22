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

#include "indexlib/base/Constant.h"
#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"
namespace indexlib::index {

class InDocPositionState
{
public:
    InDocPositionState(const PositionListFormatOption& option = PositionListFormatOption()) : _option(option)
    {
        _data.sectionReader = nullptr;
    }

    InDocPositionState(const InDocPositionState& other)
        : _docId(other._docId)
        , _termFreq(other._termFreq)
        , _seekedDocCount(other._seekedDocCount)
        , _hasSectionReader(other._hasSectionReader)
        , _option(other._option)
    {
        _data = other._data;
    }

    virtual ~InDocPositionState() {}

public:
    virtual InDocPositionIterator* CreateInDocIterator() const = 0;

    virtual void Free() = 0;

    virtual tf_t GetTermFreq()
    {
        if (_termFreq == 0) {
            _termFreq = 1;
        }
        return _termFreq;
    }

    void SetDocId(docid64_t docId) { _docId = docId; }

    docid64_t GetDocId() const { return _docId; }

    void SetTermFreq(tf_t termFreq) { _termFreq = termFreq; }

    void SetSeekedDocCount(uint32_t seekedDocCount) { _seekedDocCount = seekedDocCount; }

    uint32_t GetSeekedDocCount() const { return _seekedDocCount; }

    fieldid_t GetFieldId(int32_t fieldIdxInPack) const { return _data.sectionReader->GetFieldId(fieldIdxInPack); }

    void SetSectionReader(const SectionAttributeReader* sectionReader)
    {
        if (sectionReader) {
            _hasSectionReader = true;
            _data.sectionReader = sectionReader;
        }
    }

    const SectionAttributeReader* GetSectionReader() const
    {
        if (!_hasSectionReader) {
            return NULL;
        }
        return _data.sectionReader;
    }

protected:
    void Clone(InDocPositionState* state) const
    {
        state->_data = _data;
        state->_termFreq = _termFreq;
        state->_docId = _docId;
        state->_seekedDocCount = _seekedDocCount;
        state->_hasSectionReader = _hasSectionReader;
        state->_option = _option;
    }

protected:
    union StateData {
        // allways be const SectionAttributeReaderImpl*
        // [[deprecated("SectionAttributeReader* should not be held in this class.")]]
        const SectionAttributeReader* sectionReader;
    };

    StateData _data;                  // 8 byte
    docid64_t _docId = INVALID_DOCID; // 8 byte
    tf_t _termFreq = 0;               // 4 byte
    uint32_t _seekedDocCount = 0;     // 4 byte
    bool _hasSectionReader = false;   // 1 byte
    PositionListFormatOption _option; // 1 byte
};

} // namespace indexlib::index
