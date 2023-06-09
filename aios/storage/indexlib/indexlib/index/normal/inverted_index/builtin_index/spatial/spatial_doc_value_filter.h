/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H
#define __INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index { namespace legacy {

class SpatialDocValueFilter : public DocValueFilter
{
public:
    SpatialDocValueFilter(const std::shared_ptr<index::Shape>& shape, const LocationAttributeReaderPtr& attrReader,
                          autil::mem_pool::Pool* sessionPool)
        : DocValueFilter(sessionPool)
        , mShape(shape)
        , mAttrIter(NULL)
        , mAttrReader(attrReader)
    {
        assert(mShape);
        if (mAttrReader) {
            mAttrIter = mAttrReader->CreateIterator(_sessionPool);
        }
    }

    SpatialDocValueFilter(const SpatialDocValueFilter& other)
        : DocValueFilter(other)
        , mShape(other.mShape)
        , mAttrIter(NULL)
        , mAttrReader(other.mAttrReader)
    {
        assert(mShape);
        if (mAttrReader) {
            mAttrIter = mAttrReader->CreateIterator(_sessionPool);
        }
    }

    ~SpatialDocValueFilter()
    {
        if (mAttrIter) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, mAttrIter);
        }
    }

public:
    bool Test(docid_t docid) override { return InnerTest(docid); }

    bool InnerTest(docid_t docId)
    {
        autil::MultiDouble value;
        if (!mAttrIter || !mAttrIter->Seek(docId, value)) {
            return false;
        }
        assert(value.size() % 2 == 0);
        uint32_t cursor = 0;
        uint32_t pointCount = value.size() >> 1;
        for (uint32_t i = 0; i < pointCount; i++) {
            double lon = value[cursor++];
            double lat = value[cursor++];
            if (mShape->IsInnerCoordinate(lon, lat)) {
                return true;
            }
        }
        return false;
    }

    DocValueFilter* Clone() const override
    {
        return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SpatialDocValueFilter, *this);
    }

private:
    typedef typename VarNumAttributeReader<double>::AttributeIterator AttributeIterator;
    std::shared_ptr<index::Shape> mShape;
    AttributeIterator* mAttrIter;
    LocationAttributeReaderPtr mAttrReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialDocValueFilter);
}}} // namespace indexlib::index::legacy

#endif //__INDEXLIB_SPATIAL_DOC_VALUE_FILTER_H
