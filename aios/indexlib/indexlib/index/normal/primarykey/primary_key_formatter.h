#ifndef __INDEXLIB_PRIMARY_KEY_FORMATTER_H
#define __INDEXLIB_PRIMARY_KEY_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_pair.h"
#include "indexlib/index/normal/primarykey/primary_key_iterator.h"
#include "indexlib/index/normal/primarykey/ordered_primary_key_iterator.h"
#include "indexlib/index/segment_output_mapper.h"
#include "indexlib/util/hash_map.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(file_system, SliceFile);
IE_NAMESPACE_BEGIN(index);

struct PKMergeOutputData
{
    segmentindex_t outputIdx = 0;
    segmentindex_t targetSegmentIndex = 0;
    file_system::FileWriterPtr fileWriter;
};


template <typename Key>
class PrimaryKeyFormatter
{
public:
    typedef index::PKPair<Key> PKPair;
    typedef util::HashMap<Key, docid_t> HashMapType;
    typedef std::tr1::shared_ptr<HashMapType> HashMapTypePtr;
    typedef std::tr1::shared_ptr<PrimaryKeyIterator<Key> > PrimaryKeyIteratorPtr;
    typedef std::tr1::shared_ptr<OrderedPrimaryKeyIterator<Key> > OrderedPrimaryKeyIteratorPtr;


public :
    PrimaryKeyFormatter() {}
    virtual ~PrimaryKeyFormatter() {}

public:
    virtual void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
                        const file_system::FileWriterPtr& fileWriter) const = 0;

    virtual void Format(const OrderedPrimaryKeyIteratorPtr& pkIter,
        SegmentOutputMapper<PKMergeOutputData>& outputMapper) const = 0;

    virtual void SerializeToFile(const HashMapTypePtr& hashMap,
                                 size_t docCount,
                                 autil::mem_pool::PoolBase* pool,
                                 const file_system::FileWriterPtr& file) const = 0;
    
    virtual size_t CalculatePkSliceFileLen(size_t pkCount, size_t docCount) const = 0;
    virtual void DeserializeToSliceFile(const PrimaryKeyIteratorPtr& pkIterator,
                                        const file_system::SliceFilePtr& sliceFile,
                                        size_t sliceFileLen)
    { assert(false); }

    virtual size_t EstimatePkCount(size_t fileLength, uint32_t docCount) const = 0;
    
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARY_KEY_FORMATTER_H
