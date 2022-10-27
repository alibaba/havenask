#ifndef __INDEXLIB_SHAPE_CREATOR_H
#define __INDEXLIB_SHAPE_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"

IE_NAMESPACE_BEGIN(common);

class ShapeCreator
{
public:
    ShapeCreator();
    ~ShapeCreator();

public:
    static ShapePtr Create(const std::string& shapeName,
                           const std::string& shapeArgs);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShapeCreator);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPE_CREATOR_H
