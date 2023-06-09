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
#ifndef __INDEXLIB_VAR_LEN_DATA_ACCESSOR_H
#define __INDEXLIB_VAR_LEN_DATA_ACCESSOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/RadixTree.h"
#include "indexlib/index/common/TypedSliceList.h"
#include "indexlib/index/data_structure/var_len_data_iterator.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/HashMap.h"

namespace indexlib { namespace index {

class VarLenDataAccessor
{
public:
    typedef util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::shared_ptr<EncodeMap> EncodeMapPtr;

public:
    VarLenDataAccessor();
    ~VarLenDataAccessor();

public:
    void Init(autil::mem_pool::Pool* pool, bool uniqEncode = false);
    void InitForUniqEncode(autil::mem_pool::Pool* pool, uint32_t hashMapInitSize = HASHMAP_INIT_SIZE);

    void AppendValue(const autil::StringView& value);
    bool UpdateValue(docid_t docId, const autil::StringView& value);

    void AppendValue(const autil::StringView& value, uint64_t valueHash);
    bool UpdateValue(docid_t docId, const autil::StringView& value, uint64_t valueHash);

    VarLenDataIteratorPtr CreateDataIterator() const;

    // read raw data
    bool ReadData(const docid_t docid, uint8_t*& data, uint32_t& dataLength) const;

    uint64_t GetOffset(docid_t docid) const { return (*mOffsets)[docid]; }
    uint64_t GetDocCount() const { return mOffsets->Size(); }
    size_t GetAppendDataSize() const { return mAppendDataSize; }
    bool IsUniqEncode() const { return mEncodeMap != nullptr; }
    size_t GetUniqCount() const { return mEncodeMap == nullptr ? 0 : mEncodeMap->Size(); }

private:
    uint64_t AppendData(const autil::StringView& data);
    uint64_t GetHashValue(const autil::StringView& data);

protected:
    const static uint32_t SLICE_LEN = 1024 * 1024;
    const static uint32_t OFFSET_SLICE_LEN = 16 * 1024;
    const static uint32_t SLOT_NUM = 64;

private:
    RadixTree* mData;
    TypedSliceList<uint64_t>* mOffsets;
    size_t mAppendDataSize;
    EncodeMapPtr mEncodeMap;

private:
    friend class VarLenDataAccessorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarLenDataAccessor);
}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_LEN_DATA_ACCESSOR_H
