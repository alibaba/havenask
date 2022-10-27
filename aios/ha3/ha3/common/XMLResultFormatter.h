#ifndef ISEARCH_XMLRESULTFORMATTER_H
#define ISEARCH_XMLRESULTFORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/ResultFormatter.h>
#include <sstream>

BEGIN_HA3_NAMESPACE(common);

class XMLResultFormatter : public ResultFormatter
{
public:
    XMLResultFormatter();
    ~XMLResultFormatter();
public:
    void format(const ResultPtr &result,
                       std::stringstream &ss);
    static std::string xmlFormatResult(const ResultPtr &result);
    static std::string xmlFormatErrorResult(const ErrorResult &error);
    static void addCacheInfo(std::string &resultString, bool isFromCache);

private:
    static void fillHeader(std::stringstream &ss);

    static void formatHits(const ResultPtr &resultPtr, std::stringstream &ss);
    static void formatMeta(const ResultPtr &resultPtr, std::stringstream &ss);
    static void formatSortExpressionMeta(const ResultPtr &resultPtr,
                                  std::stringstream &ss);
    static void formatAggregateResults(const ResultPtr &resultPtr,
                                std::stringstream &ss);
    static void formatAggregateResult(const AggregateResultPtr &aggResultPtr,
                               std::stringstream &ss);

    static void formatErrorResult(const ResultPtr &resultPtr,
                                  std::stringstream &ss);

    static void formatTotalTime(const ResultPtr &resultPtr,
                                std::stringstream &ss);

    static void formatRequestTracer(const ResultPtr &resultPtr,
                             std::stringstream &ss);

    static void formatAttributeMap(const AttributeMap &attributeMap,
                                   const std::string &tag,
                                   std::stringstream &ss);

    static std::string getCoveredPercentStr(const Result::ClusterPartitionRanges &ranges);

private:
    friend class XMLResultFormatterTest;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);


#endif //ISEARCH_XMLRESULTFORMATTER_H
