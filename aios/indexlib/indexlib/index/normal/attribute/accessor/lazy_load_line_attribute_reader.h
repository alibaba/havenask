#ifndef __INDEXLIB_LAZY_LOAD_LINE_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_LINE_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"
#include "indexlib/common/field_format/attribute/shape_attribute_util.h"

IE_NAMESPACE_BEGIN(index);

class LazyLoadLineAttributeReader : public LazyLoadVarNumAttributeReader<double>
{
public:
    LazyLoadLineAttributeReader(AttributeMetrics* metrics = NULL)
        : LazyLoadVarNumAttributeReader<double>(metrics)
    {}
    ~LazyLoadLineAttributeReader() {}
public:
    class Creator : public AttributeReaderCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return ft_line;
    }

    AttributeReader* Create(AttributeMetrics* metrics = NULL) const
    {
        return new LazyLoadLineAttributeReader(metrics);
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
                common::Shape::LINE, value, attrValue);
    }

private:
IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LazyLoadLineAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_LINE_ATTRIBUTE_READER_H
