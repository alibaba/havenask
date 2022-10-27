#ifndef __INDEXLIB_ATTRIBUTE_READER_H
#define __INDEXLIB_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/mem_pool/Pool.h>
#include <autil/ConstString.h>

DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(index, PrimaryKeyIndexReader);
DECLARE_REFERENCE_CLASS(index, JoinDocidCacheReader);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeIteratorBase);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(index_base, SegmentData);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, RangeDescription);
DECLARE_REFERENCE_CLASS(file_system, Directory);

IE_NAMESPACE_BEGIN(index);

#define DECLARE_ATTRIBUTE_READER_IDENTIFIER(id)                         \
    static std::string Identifier() { return std::string("attribute.reader." #id);} \
    virtual std::string GetIdentifier() const override { return Identifier();}

class AttributeReader
{
public:
    AttributeReader(AttributeMetrics* metrics = NULL)
        : mAttributeMetrics(metrics)
    {}
    virtual ~AttributeReader() {}

public:
    virtual bool Open(const config::AttributeConfigPtr& attrConfig,
                      const index_base::PartitionDataPtr& partitionData) = 0;

    virtual bool IsLazyLoad() const { return false; }
    
    // TODO: remove
    virtual AttributeReader* Clone() const
    { assert(false); return NULL; }

    virtual bool Read(docid_t docId, std::string& attrValue,
                      autil::mem_pool::Pool* pool = NULL) const = 0;
    
    virtual bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const = 0;

    /**
     * Get attribute type
     */
    virtual AttrType GetType() const = 0;

    /**
     * Get multi value flag 
     */
    virtual bool IsMultiValue() const = 0;

    virtual std::string GetIdentifier() const = 0;

    virtual AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool *pool) const = 0;

    virtual bool GetSortedDocIdRange(
            const index_base::RangeDescription& range,
            const DocIdRange& rangeLimit, 
            DocIdRange& resultRange) const = 0;

    virtual bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen)
    {
        return false;
    }

    virtual std::string GetAttributeName() const
    {
        assert(false);
        return "";
    }

    virtual JoinDocidCacheReader* CreateJoinDocidCacheReader(
            const PrimaryKeyIndexReaderPtr& pkIndexReader,
            const DeletionMapReaderPtr& deletionMapReader,
            const AttributeReaderPtr& attributeReader = AttributeReaderPtr())
    { 
        assert(false);
        return NULL;
    }

    virtual size_t EstimateLoadSize(const index_base::PartitionDataPtr& partitionData,
                                    const config::AttributeConfigPtr& attrConfig,
                                    const index_base::Version& lastLoadVersion)
    {
        assert(false);
        return 0;
    }
    virtual AttributeSegmentReaderPtr GetSegmentReader(docid_t docId) const
    {
        assert(false);
        return AttributeSegmentReaderPtr();
    };

protected:
    // some attribute base directory may be not attribute directory
    // e.g. pk attribute, section 
    virtual file_system::DirectoryPtr GetAttributeDirectory(
            const index_base::SegmentData& segmentData,
            const config::AttributeConfigPtr& attrConfig) const
    {
        return file_system::DirectoryPtr();
    }

protected:
    AttributeMetrics* mAttributeMetrics;
};

typedef std::map<std::string, size_t> AttributeName2PosMap;
typedef std::vector<AttributeReaderPtr> AttributeReaderVector;

DEFINE_SHARED_PTR(PrimaryKeyIndexReader);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_READER_H
