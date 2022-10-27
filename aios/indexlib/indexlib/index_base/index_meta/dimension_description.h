#ifndef __INDEXLIB_DIMENSION_DESCRIPTION_H
#define __INDEXLIB_DIMENSION_DESCRIPTION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/StringUtil.h>

IE_NAMESPACE_BEGIN(index_base);

struct RangeDescription 
{
    RangeDescription() {}
    RangeDescription(const std::string& from, 
                     const std::string& to)
        : from(from)
        , to(to)
    {
    }
    std::string from;
    std::string to;
    static const std::string INFINITE;
};

struct DimensionDescription 
{
    std::vector<RangeDescription> ranges;
    std::vector<std::string> values;
    std::string name;
};

DEFINE_SHARED_PTR(DimensionDescription);

typedef std::vector<DimensionDescriptionPtr> DimensionDescriptionVector;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_DIMENSION_DESCRIPTION_H
