#ifndef __INDEXLIB_REGULAR_EXPRESSION_H
#define __INDEXLIB_REGULAR_EXPRESSION_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <sys/types.h>
#include <regex.h>

IE_NAMESPACE_BEGIN(misc);

class RegularExpression
{
public:
    RegularExpression();
    ~RegularExpression();

public:
    bool Init(const std::string &pattern);
    bool Match(const std::string &string) const;
    std::string GetLatestErrMessage() const;

private:
    regex_t mRegex;
    mutable int mLatestErrorCode;
    bool mInitialized;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(RegularExpression);
typedef std::vector<RegularExpressionPtr> RegularExpressionVector;

IE_NAMESPACE_END(misc);

#endif //__INDEXLIB_REGULAR_EXPRESSION_H
