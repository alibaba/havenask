#ifndef __INDEXLIB_FAKE_ATTRIBUTE_READER_H
#define __INDEXLIB_FAKE_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/merger/segment_directory.h"

IE_NAMESPACE_BEGIN(partition);

class FakeAttributeReader : public AttributeReader
{
public:
    static const versionid_t BAD_VERSION_OF_ATTRIBUTE = -12;

public:
    FakeAttributeReader(FieldType fieldType, bool isMultiValue) 
        : mFieldType(fieldType)
        , mIsMultiValue(isMultiValue)
        , mHasBuildingAttributeReader(false)
    {
    }
    FakeAttributeReader(const FakeAttributeReader& reader) {}
    ~FakeAttributeReader() {}
    
    DECLARE_ATTRIBUTE_READER_IDENTIFIER(fake);

public:
    void AddAttributeSegmentReader(
            const AttributeSegmentReaderPtr& attrSegReader)
    {
        mHasBuildingAttributeReader = true;
    }

    virtual bool Open(const config::AttributeConfigPtr& attrConfig,
                      const index_base::PartitionDataPtr& partitionData) override
    { return true; }

    bool ReOpen(const SegmentDirectoryPtr& segDir) 
    {
        if (segDir->GetVersion().GetVersionId() == BAD_VERSION_OF_ATTRIBUTE)
        {
            return false;
        }
        return true;
    }

    bool GetSortedDocIdRange(const index_base::RangeDescription& range,
                             const DocIdRange& rangeLimit, 
                             DocIdRange& resultRange) const
    {
        return false;
    }
    
    AttributeReader* Clone() const { return new FakeAttributeReader(*this); }
    bool Read(docid_t docId, std::string& attrValue) const {return true;}
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) const 
    { assert(false); return false; }
    AttrType GetType() const { return AT_UNKNOWN; }
    bool IsMultiValue() const { return mIsMultiValue; }
    bool IsUpdatable() const { return false; }
    AttributeIteratorBase* CreateIterator(autil::mem_pool::Pool *pool) const {return NULL;};
    void WarmUp() const {}
    bool HasBuildingAttributeReader() { return mHasBuildingAttributeReader; }
    
private:
    FieldType mFieldType;
    bool mIsMultiValue;
    bool mHasBuildingAttributeReader;
};

DEFINE_SHARED_PTR(FakeAttributeReader);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_FAKE_ATTRIBUTE_READER_H
