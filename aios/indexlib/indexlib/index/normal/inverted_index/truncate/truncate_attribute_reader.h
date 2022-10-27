#ifndef __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H
#define __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/reclaim_map/reclaim_map.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/index/normal/attribute/accessor/attribute_segment_reader.h"

IE_NAMESPACE_BEGIN(index);

class TruncateAttributeReader : public AttributeSegmentReader
{
public:
    TruncateAttributeReader();
    ~TruncateAttributeReader();
public:
    bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) override;
    bool IsInMemory() const override { assert(false); return false; }
    uint32_t GetDataLength(docid_t docId) const override { assert(false); return 0; }
    uint64_t GetOffset(docid_t docId) const override { assert(false); return 0; }

    void Init(const ReclaimMapPtr &relaimMap, 
              const config::AttributeConfigPtr& attrConfig);
    void AddAttributeSegmentReader(segmentid_t segId, 
                                   const AttributeSegmentReaderPtr& segmentReader);
    config::AttributeConfigPtr GetAttributeConfig() const { return mAttrConfig; }
    
private:
    //TODO: it'll use too much memory while inc building many times
    typedef std::vector<AttributeSegmentReaderPtr> AttributeSegmentReaderMap;
    AttributeSegmentReaderMap mReaders;
    ReclaimMapPtr mReclaimMap;
    config::AttributeConfigPtr mAttrConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_ATTRIBUTE_READER_H
