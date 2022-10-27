#ifndef ISEARCH_SQLSEARCHINFOCOLLECTOR_H
#define ISEARCH_SQLSEARCHINFOCOLLECTOR_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/Lock.h>
#include <ha3/sql/proto/SqlSearchInfo.pb.h>

BEGIN_HA3_NAMESPACE(sql);

class SqlSearchInfoCollector
{
public:
    SqlSearchInfoCollector();
    ~SqlSearchInfoCollector();
private:
    SqlSearchInfoCollector(const SqlSearchInfoCollector &);
    SqlSearchInfoCollector& operator=(const SqlSearchInfoCollector &);
public:
    void addScanInfo(const ScanInfo &scanInfo);
    void addJoinInfo(const JoinInfo &joinInfo);
    void addAggInfo(const AggInfo &aggInfo);
    void addSortInfo(const SortInfo &sortInfo);
    void addCalcInfo(const CalcInfo &calcInfo);
    void addTvfInfo(const TvfInfo &tvfInfo);
    void addExchangeInfo(const ExchangeInfo &exchangeInfo);

    void addRpcInfo(const RpcInfo &rpcInfo);
    RpcInfo* addRpcInfo();
    void setForbitMerge(bool flag);
    bool getForbitMerge();

    SqlSearchInfo getSqlSearchInfo();
    void mergeInSearch();
    void mergeInQrs();

    void serializeToString(std::string &searchInfoStr);
    bool deserializeFromString(const std::string &searchInfoStr);
    void mergeSqlSearchInfo(const SqlSearchInfo &sqlSearchInfo);
private:
    void mergeScanInfo();
    void mergeJoinInfo();
    void mergeAggInfo();
    void mergeSortInfo();
    void mergeCalcInfo();
    void mergeTvfInfo();
private:
    autil::ThreadMutex _mutex;
    SqlSearchInfo _sqlSearchInfo;
    bool _forbitMerge;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SqlSearchInfoCollector);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQLSEARCHINFOCOLLECTOR_H
