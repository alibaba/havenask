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

#include "autil/StringView.h"
#include "indexlib/index/common/RadixTree.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/common/data_structure/VarLenDataIterator.h"
#include "indexlib/util/HashMap.h"

namespace indexlibv2::index {

class VarLenDataAccessor
{
public:
    typedef indexlib::util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::shared_ptr<EncodeMap> EncodeMapPtr;

public:
    VarLenDataAccessor();
    ~VarLenDataAccessor();

public:
    void Init(autil::mem_pool::Pool* pool, bool uniqEncode = false);
    void InitForUniqEncode(autil::mem_pool::Pool* pool, uint32_t hashMapInitSize);

    void AppendValue(const autil::StringView& value);
    bool UpdateValue(docid_t docId, const autil::StringView& value);

    void AppendValue(const autil::StringView& value, uint64_t valueHash);
    bool UpdateValue(docid_t docId, const autil::StringView& value, uint64_t valueHash);

    std::shared_ptr<VarLenDataIterator> CreateDataIterator() const;

    // read raw data
    bool ReadData(const docid_t docid, uint8_t*& data, uint32_t& dataLength) const;

    uint64_t GetOffset(docid_t docid) const { return (*_offsets)[docid]; }
    uint64_t GetDocCount() const { return _offsets->Size(); }
    size_t GetAppendDataSize() const { return _appendDataSize; }
    bool IsUniqEncode() const { return _encodeMap != nullptr; }
    size_t GetUniqCount() const { return _encodeMap == nullptr ? 0 : _encodeMap->Size(); }

private:
    uint64_t AppendData(const autil::StringView& data);
    uint64_t GetHashValue(const autil::StringView& data);

protected:
    const static uint32_t SLICE_LEN = 1024 * 1024;
    const static uint32_t OFFSET_SLICE_LEN = 16 * 1024;
    const static uint32_t SLOT_NUM = 64;

private:
    indexlib::index::RadixTree* _data;
    indexlib::index::TypedSliceList<uint64_t>* _offsets;
    size_t _appendDataSize;
    EncodeMapPtr _encodeMap;

private:
    friend class VarLenDataAccessorTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
