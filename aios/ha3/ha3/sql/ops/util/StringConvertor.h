#pragma once
#include <ha3/sql/common/common.h>
#include <autil/ConstStringUtil.h>
#include <autil/MultiValueCreator.h>
#include <autil/StringTokenizer.h>

BEGIN_HA3_NAMESPACE(sql);

class StringConvertor {
public:
    template <typename T>
    static bool fromString(const autil::ConstString *value,
                           T &val, autil::mem_pool::Pool *pool);
};

template <typename T>
inline bool StringConvertor::fromString(const autil::ConstString *value,
                                 T &val, autil::mem_pool::Pool *pool)
{
    return autil::ConstStringUtil::fromString(value, val, pool);
}

template <>
inline bool StringConvertor::fromString(const autil::ConstString *value,
                                 autil::MultiString &val, autil::mem_pool::Pool *pool)
{
     if (value->empty()) {
         std::vector<std::string> vec;
         char *buffer = autil::MultiValueCreator::createMultiValueBuffer(
                 vec, pool);
         val.init(buffer);
     } else {
         std::vector<autil::ConstString> vec = autil::StringTokenizer::constTokenize(
                 *value, MULTI_VALUE_SEPARATOR, autil::StringTokenizer::TOKEN_LEAVE_AS);
         char *buffer = autil::MultiValueCreator::createMultiValueBuffer(vec, pool);
         val.init(buffer);
     }
     return true;
}

END_HA3_NAMESPACE(sql);
