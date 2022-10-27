#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_ACCESSOR_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_ACCESSOR_H

#include <tr1/memory>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/radix_tree.h"
#include "indexlib/common/typed_slice_list.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/util/hash_map.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_data_iterator.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeAccessor
{
public:
    typedef util::HashMap<uint64_t, uint64_t> EncodeMap;
    typedef std::tr1::shared_ptr<EncodeMap> EncodeMapPtr;
public:
    VarNumAttributeAccessor()
        : mData(NULL)
        , mOffsets(NULL)
        , mAppendDataSize(0)
    {}

    ~VarNumAttributeAccessor() {}

public:
    void Init(autil::mem_pool::Pool *pool);
    void AddEncodedField(const common::AttrValueMeta& meta, EncodeMapPtr& encodeMap);
    void AddNormalField(const common::AttrValueMeta& meta);
    bool UpdateEncodedField(const docid_t docid, const common::AttrValueMeta& meta,
                            EncodeMapPtr& encodeMap);
    bool UpdateNormalField(const docid_t docid, const common::AttrValueMeta& meta);
    VarNumAttributeDataIteratorPtr CreateVarNumAttributeDataIterator() const; 
    
    // read raw data(including COUNT)
    void ReadData(
            const docid_t docid, uint8_t*& data, uint32_t& dataLength) const;

    uint64_t GetOffset(docid_t docid) const
    { return (*mOffsets)[docid]; }

    uint64_t GetDocCount() const { return mOffsets->Size(); }

    void Append(const autil::ConstString& data);

    size_t GetAppendDataSize() const { return mAppendDataSize; }

private:
    uint64_t AppendData(const autil::ConstString& data);

protected:
    const static uint32_t SLICE_LEN = 1024 * 1024;
    const static uint32_t OFFSET_SLICE_LEN = 16 * 1024;
    const static uint32_t SLOT_NUM = 64;

private:
    common::RadixTree* mData;
    common::TypedSliceList<uint64_t>* mOffsets;
    size_t mAppendDataSize;

private:
    IE_LOG_DECLARE();
    friend class VarNumAttributeAccessorTest;
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_ACCESSOR_H
