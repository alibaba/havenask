#ifndef __INDEXLIB_POLYGON_ATTRIBUTE_READER_H
#define __INDEXLIB_POLYGON_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class PolygonAttributeReader : public VarNumAttributeReader<double>
{
public:
    PolygonAttributeReader(AttributeMetrics* metrics = NULL)
        : VarNumAttributeReader<double>(metrics)
    {}
    ~PolygonAttributeReader() {}
public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const override
        {
            return ft_polygon;
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const override
        {
            return new PolygonAttributeReader(metrics);
        }
    };

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool) const override;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PolygonAttributeReader);
/////////////////////////////////////////////////////
bool PolygonAttributeReader::Read(
        docid_t docId, std::string& attrValue,
        autil::mem_pool::Pool* pool = NULL) const
{
    autil::MultiValueType<double> value;
    bool ret = VarNumAttributeReader<double>::Read(docId, value, pool);
    if (!ret)
    {
        return false;
    }
    return common::ShapeAttributeUtil::DecodeAttrValueToString(
        common::Shape::POLYGON, value, attrValue);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POLYGON_ATTRIBUTE_READER_H
