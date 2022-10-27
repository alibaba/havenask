#ifndef __INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H
#define __INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/index/normal/inverted_index/accessor/doc_value_filter.h"
#include "indexlib/index/normal/attribute/accessor/location_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class SpatialDocValueFilter : public DocValueFilter
{
public:
    SpatialDocValueFilter(const common::ShapePtr& shape,
                          const LocationAttributeReaderPtr& attrReader,
                          autil::mem_pool::Pool* sessionPool)
        : DocValueFilter(sessionPool)
        , mShape(shape)
        , mAttrIter(NULL)
        , mAttrReader(attrReader)
    {
        assert(mShape);
        if (mAttrReader)
        {
            mAttrIter = mAttrReader->CreateIterator(mSessionPool);
        }
    }

    SpatialDocValueFilter(const SpatialDocValueFilter& other)
        : DocValueFilter(other)
        , mShape(other.mShape)
        , mAttrIter(NULL)
        , mAttrReader(other.mAttrReader)
    {
        assert(mShape);
        if (mAttrReader)
        {
            mAttrIter = mAttrReader->CreateIterator(mSessionPool);
        }
    }

    ~SpatialDocValueFilter()
    {
        if (mAttrIter)
        {
            IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mAttrIter);
        }
    }
    
public:
    bool Test(docid_t docid) override { return InnerTest(docid); }
    
    bool InnerTest(docid_t docId)
    {
        autil::MultiDouble value;
        if (!mAttrIter || !mAttrIter->Seek(docId, value))
        {
            return false;
        }
        assert(value.size() % 2 == 0);
        uint32_t cursor = 0;
        uint32_t pointCount = value.size() >> 1;
        for (uint32_t i = 0; i < pointCount; i++)
        {
            double lon = value[cursor++];
            double lat = value[cursor++];
            if (mShape->IsInnerCoordinate(lon, lat))
            {
                return true;
            }
        }
        return false;
    }
        
    DocValueFilter* Clone() const override
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
                SpatialDocValueFilter, *this);
    }

private:
    typedef typename VarNumAttributeReader<double>::AttributeIterator AttributeIterator;
    common::ShapePtr mShape;
    AttributeIterator* mAttrIter;
    LocationAttributeReaderPtr mAttrReader;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialDocValueFilter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H
