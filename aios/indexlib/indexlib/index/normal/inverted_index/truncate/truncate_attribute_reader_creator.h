#ifndef __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_CREATOR_H
#define __INDEXLIB_TRUNCATE_ATTRIBUTE_READER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"

DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, TruncateAttributeReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(config, AttributeSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);

class TruncateAttributeReaderCreator
{
public:
    TruncateAttributeReaderCreator(const SegmentDirectoryBasePtr &segmentDirectory,
                                   const config::AttributeSchemaPtr& attrSchema,
                                   const index_base::SegmentMergeInfos& segMergeInfos,
                                   const ReclaimMapPtr &reclaimMap);
    ~TruncateAttributeReaderCreator();
public:
    TruncateAttributeReaderPtr Create(const std::string &attrName);
    const config::AttributeSchemaPtr &GetAttributeSchema() const { return mAttrSchema; }
    const ReclaimMapPtr &GetReclaimMap() const { return mReclaimMap; }
private:
    TruncateAttributeReaderPtr CreateAttrReader(const std::string& fieldName);
    AttributeSegmentReaderPtr CreateAttrSegmentReader(
            const config::AttributeConfigPtr& attrConfig,
            const index_base::SegmentMergeInfo& segMergeInfo);
private:
    template<typename T>
    AttributeSegmentReaderPtr CreateAttrSegmentReaderTyped(
            const config::AttributeConfigPtr& attrConfig,
            const index_base::SegmentMergeInfo& segMergeInfo);

private:
    typedef std::map<std::string, TruncateAttributeReaderPtr> TruncateAttributeReaders;

private:
    SegmentDirectoryBasePtr mSegmentDirectory;
    config::AttributeSchemaPtr mAttrSchema;
    index_base::SegmentMergeInfos mSegMergeInfos;
    ReclaimMapPtr mReclaimMap;
    TruncateAttributeReaders mTruncateAttributeReaders;
    autil::ThreadMutex mMutex;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateAttributeReaderCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_ATTRIBUTE_READER_CREATOR_H
