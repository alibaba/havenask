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
#pragma once

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/memory.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/MultiValueAttributeReader.h"
#include "indexlib/index/common/field_format/spatial/shape/Shape.h"
#include "indexlib/index/inverted_index/DocValueFilter.h"

namespace indexlib::index {

class SpatialDocValueFilter : public DocValueFilter
{
private:
    using LocationAttributeReader = indexlibv2::index::MultiValueAttributeReader<double>;
    using LocationAttrIter = LocationAttributeReader::AttributeIterator;

public:
    SpatialDocValueFilter(const std::shared_ptr<Shape>& shape, LocationAttributeReader* attributeReader,
                          autil::mem_pool::Pool* sessionPool)
        : DocValueFilter(sessionPool)
        , _shape(shape)
        , _attrReader(attributeReader)
    {
        assert(_shape);
        assert(_attrReader);
        assert(_sessionPool);
        _attrIter = autil::shared_ptr_pool_deallocated(_sessionPool, _attrReader->CreateIterator(_sessionPool));
    }

    SpatialDocValueFilter(const SpatialDocValueFilter& other)
        : DocValueFilter(other)
        , _shape(other._shape)
        , _attrReader(other._attrReader)
    {
        assert(_shape);
        _attrIter = autil::shared_ptr_pool_deallocated(_sessionPool, _attrReader->CreateIterator(_sessionPool));
    }

    ~SpatialDocValueFilter() = default;

public:
    bool Test(docid_t docid) override { return InnerTest(docid); }

    bool InnerTest(docid_t docId)
    {
        autil::MultiDouble value;
        assert(_attrIter);
        if (!_attrIter->Seek(docId, value)) {
            return false;
        }
        assert(value.size() % 2 == 0);
        uint32_t cursor = 0;
        uint32_t pointCount = value.size() >> 1;
        for (uint32_t i = 0; i < pointCount; i++) {
            double lon = value[cursor++];
            double lat = value[cursor++];
            if (_shape->IsInnerCoordinate(lon, lat)) {
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
    std::shared_ptr<Shape> _shape;
    std::shared_ptr<LocationAttrIter> _attrIter;
    LocationAttributeReader* _attrReader = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
