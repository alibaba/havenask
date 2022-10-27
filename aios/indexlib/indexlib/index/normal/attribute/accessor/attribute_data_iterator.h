#ifndef __INDEXLIB_ATTRIBUTE_DATA_ITERATOR_H
#define __INDEXLIB_ATTRIBUTE_DATA_ITERATOR_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include <autil/mem_pool/Pool.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(index);

class AttributeDataIterator
{
public:
    AttributeDataIterator(const config::AttributeConfigPtr& attrConfig);
    virtual ~AttributeDataIterator();
    
public:
    virtual bool Init(const index_base::PartitionDataPtr& partData,
                      segmentid_t segId) = 0;
    virtual void MoveToNext() = 0;
    virtual std::string GetValueStr() const = 0;

    // from T & MultiValue<T>
    virtual autil::ConstString GetValueBinaryStr(autil::mem_pool::Pool *pool) const = 0;

    // from raw index data, maybe not MultiValue<T> (compress float)
    // attention: return value lifecycle only valid in current value pos,
    //            which will be changed when call MoveToNext
    virtual autil::ConstString GetRawIndexImageValue() const = 0;
    
    bool IsValid() const { return mCurDocId < mDocCount; }

    config::AttributeConfigPtr GetAttrConfig() const { return mAttrConfig; }
    uint32_t GetDocCount() const { return (uint32_t)mDocCount; }


protected:
    config::AttributeConfigPtr mAttrConfig;
    docid_t mDocCount;
    docid_t mCurDocId;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_DATA_ITERATOR_H
