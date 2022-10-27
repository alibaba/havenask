#ifndef __INDEXLIB_SUMMARY_DEFINE_H
#define __INDEXLIB_SUMMARY_DEFINE_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_accessor.h"

IE_NAMESPACE_BEGIN(index);


struct SummaryString
{
    SummaryString() 
        : len(0)
        , value(NULL)
    {}
    uint32_t len;
    char* value;
};
typedef std::vector<SummaryString> SummaryVector;
typedef VarNumAttributeAccessor SummaryDataAccessor;
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SUMMARY_DEFINE_H
