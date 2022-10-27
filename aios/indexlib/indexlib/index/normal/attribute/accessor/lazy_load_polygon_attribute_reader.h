#ifndef __INDEXLIB_LAZY_LOAD_POLYGON_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_POLYGON_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"

IE_NAMESPACE_BEGIN(index);

class LazyLoadPolygonAttributeReader : public LazyLoadVarNumAttributeReader<double>
{
public:
    LazyLoadPolygonAttributeReader(AttributeMetrics* metrics = NULL)
        : LazyLoadVarNumAttributeReader<double>(metrics)
    {}
    ~LazyLoadPolygonAttributeReader() {}
public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_polygon;
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const
        {
            return new LazyLoadPolygonAttributeReader(metrics);
        }
    };

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        autil::MultiValueType<double> value;
        if (!LazyLoadVarNumAttributeReader<double>::Read(docId, value, pool))
        {
            return false;
        }
        return common::ShapeAttributeUtil::DecodeAttrValueToString(
                common::Shape::POLYGON, value, attrValue);
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LazyLoadPolygonAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_POLYGON_ATTRIBUTE_READER_H
