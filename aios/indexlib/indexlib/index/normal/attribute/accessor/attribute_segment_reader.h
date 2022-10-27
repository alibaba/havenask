#ifndef __INDEXLIB_ATTRIBUTE_SEGMENT_READER_H
#define __INDEXLIB_ATTRIBUTE_SEGMENT_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, AttributeMetrics);
DECLARE_REFERENCE_CLASS(index, AttributePatchReader);

IE_NAMESPACE_BEGIN(index);

class AttributeSegmentReader
{
public:
    AttributeSegmentReader(AttributeMetrics* attributeMetrics = NULL)
        : mAttributeMetrics(attributeMetrics)
    {}
    virtual ~AttributeSegmentReader() {}

public:
    virtual bool IsInMemory() const = 0;
    virtual uint32_t GetDataLength(docid_t docId) const = 0;
    virtual uint64_t GetOffset(docid_t docId) const = 0;
    virtual bool Read(docid_t docId, uint8_t* buf, uint32_t bufLen) = 0;

    virtual bool ReadDataAndLen(docid_t docId, uint8_t* buf, 
                                uint32_t bufLen, uint32_t &dataLen)
    {
        dataLen = bufLen;
        return Read(docId, buf, bufLen);
    }
    virtual bool UpdateField(docid_t docId, uint8_t* buf, uint32_t bufLen)
    {
        return false;
    }

protected:
    void LoadPatches(const AttributePatchReaderPtr& patchIterator);

protected:
    AttributeMetrics* mAttributeMetrics;
};

DEFINE_SHARED_PTR(AttributeSegmentReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_SEGMENT_READER_H
