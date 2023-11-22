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

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_traits.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

template <typename T, typename ReaderTraits = AttributeReaderTraits<T>>
class BuildingAttributeReader
{
public:
    typedef typename ReaderTraits::InMemSegmentReader InMemSegmentReader;
    typedef std::shared_ptr<InMemSegmentReader> InMemSegmentReaderPtr;

public:
    BuildingAttributeReader() {}
    ~BuildingAttributeReader() {}

public:
    void AddSegmentReader(docid_t baseDocId, const InMemSegmentReaderPtr& inMemSegReader)
    {
        if (inMemSegReader) {
            mBaseDocIds.push_back(baseDocId);
            mSegmentReaders.push_back(inMemSegReader);
        }
    }

    template <typename ValueType>
    inline bool Read(docid_t docId, ValueType& value, size_t& idx, bool& isNull,
                     autil::mem_pool::Pool* pool = NULL) const __ALWAYS_INLINE;

    inline bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen, size_t& idx, bool& isNull) __ALWAYS_INLINE;
    inline bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen, bool isNull);

    docid_t GetBaseDocId(size_t idx) const { return idx < mBaseDocIds.size() ? mBaseDocIds[idx] : INVALID_DOCID; }

    size_t Size() const { return mBaseDocIds.size(); }

private:
    std::vector<docid_t> mBaseDocIds;
    std::vector<InMemSegmentReaderPtr> mSegmentReaders;

private:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////
template <typename T, typename ReaderTraits>
template <typename ValueType>
inline bool BuildingAttributeReader<T, ReaderTraits>::Read(docid_t docId, ValueType& value, size_t& idx, bool& isNull,
                                                           autil::mem_pool::Pool* pool) const
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId) {
            return false;
        }
        if (mSegmentReaders[i]->Read(docId - curBaseDocId, value, isNull, pool)) {
            idx = i;
            return true;
        }
    }
    return false;
}

template <typename T, typename ReaderTraits>
inline bool BuildingAttributeReader<T, ReaderTraits>::Read(docid_t docId, uint8_t* buf, uint32_t bufLen, size_t& idx,
                                                           bool& isNull)
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId) {
            return false;
        }
        if (mSegmentReaders[i]->Read(docId - curBaseDocId, buf, bufLen, isNull)) {
            idx = i;
            return true;
        }
    }
    return false;
}

template <typename T, typename ReaderTraits>
inline bool BuildingAttributeReader<T, ReaderTraits>::UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen,
                                                                  bool isNull)
{
    for (size_t i = 0; i < mSegmentReaders.size(); i++) {
        docid_t curBaseDocId = mBaseDocIds[i];
        if (docId < curBaseDocId) {
            return false;
        }
        if (mSegmentReaders[i]->UpdateField(docId - curBaseDocId, buf, bufLen, isNull)) {
            return true;
        }
    }
    return false;
}
}} // namespace indexlib::index
