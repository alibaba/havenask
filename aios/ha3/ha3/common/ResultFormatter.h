#ifndef ISEARCH_RESULTFORMATTER_H
#define ISEARCH_RESULTFORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Result.h>
#include <sstream>

BEGIN_HA3_NAMESPACE(common);

class ResultFormatter
{
public:
    ResultFormatter() {};
    virtual ~ResultFormatter() {};
public:
    virtual void format(const ResultPtr &result, 
                        std::stringstream &retString) = 0;
    static double getCoveredPercent(const Result::ClusterPartitionRanges &ranges);
};

END_HA3_NAMESPACE(common);
#endif //ISEARCH_RESULTFORMATTER_H
