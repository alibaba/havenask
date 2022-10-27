#ifndef __INDEXLIB_ATTRIBUTE_PATCH_READER_H
#define __INDEXLIB_ATTRIBUTE_PATCH_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);

class AttributePatchReader
{
public:
    AttributePatchReader(const config::AttributeConfigPtr& attrConfig)
        : mAttrConfig(attrConfig)
    {}
    
    virtual ~AttributePatchReader() {}

public:
    virtual void Init(const index_base::PartitionDataPtr& partitionData,
                      segmentid_t segmentId);

public:
    virtual void AddPatchFile(const file_system::DirectoryPtr& directory,
                              const std::string& fileName,
                              segmentid_t srcSegmentId) = 0;

    virtual docid_t GetCurrentDocId() const = 0;
    virtual size_t GetPatchFileLength() const = 0;
    virtual size_t GetPatchItemCount() const = 0;
    virtual size_t Next(docid_t& docId, uint8_t* buffer,
                        size_t bufferLen) = 0;

    virtual size_t Seek(docid_t docId, uint8_t* value,
                        size_t maxLen) = 0;
    
    virtual bool HasNext() const = 0;

    virtual uint32_t GetMaxPatchItemLen() const = 0;

protected:
    config::AttributeConfigPtr mAttrConfig;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchReader);

typedef AttributePatchReader AttributeSegmentPatchIterator;
typedef AttributePatchReaderPtr AttributeSegmentPatchIteratorPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_READER_H
