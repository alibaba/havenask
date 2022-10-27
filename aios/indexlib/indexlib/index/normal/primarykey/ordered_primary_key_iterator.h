#ifndef __INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H
#define __INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"

IE_NAMESPACE_BEGIN(index);

template <typename Key>
class OrderedPrimaryKeyIterator : public PrimaryKeyIterator<Key>
{
public:
    OrderedPrimaryKeyIterator(const config::IndexConfigPtr& indexConfig)
        : PrimaryKeyIterator<Key>(indexConfig)
    {}
    virtual ~OrderedPrimaryKeyIterator() {}

public:
    virtual uint64_t GetPkCount() const { return 0; } 
    virtual uint64_t GetDocCount() const { return  0; }
    
/*    typedef PKPair<Key> PKPair; */
    
/* public: */
/*     virtual void Init(const std::vector<index_base::SegmentData>& segmentDatas) = 0; */
/*     virtual bool HasNext() const = 0; */
/*     virtual void Next(PKPair& pair) =  0; */

/* protected: */
/*     config::IndexConfigPtr mIndexConfig; */
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ORDERED_PRIMARY_KEY_ITERATOR_H
