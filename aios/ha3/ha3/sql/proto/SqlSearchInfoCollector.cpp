#include <ha3/sql/proto/SqlSearchInfoCollector.h>
#include <autil/StringUtil.h>

using namespace std;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(proto, SqlSearchInfoCollector);

SqlSearchInfoCollector::SqlSearchInfoCollector() {
    _forbitMerge = false;
}

SqlSearchInfoCollector::~SqlSearchInfoCollector() {
}

void SqlSearchInfoCollector::addRpcInfo(const RpcInfo &rpcInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_rpcinfos()) = rpcInfo;
}

RpcInfo* SqlSearchInfoCollector::addRpcInfo() {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo.add_rpcinfos();
}

void SqlSearchInfoCollector::setForbitMerge(bool flag) {
    _forbitMerge = flag;
}

bool SqlSearchInfoCollector::getForbitMerge() {
    return _forbitMerge;
}

void SqlSearchInfoCollector::addScanInfo(const ScanInfo &scanInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_scaninfos()) = scanInfo;
}

void SqlSearchInfoCollector::addJoinInfo(const JoinInfo &joinInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_joininfos()) = joinInfo;
}

void SqlSearchInfoCollector::addAggInfo(const AggInfo &aggInfo) {
   autil::ScopedLock lock(_mutex);
   *(_sqlSearchInfo.add_agginfos()) = aggInfo;
}

void SqlSearchInfoCollector::addSortInfo(const SortInfo &sortInfo) {
   autil::ScopedLock lock(_mutex);
   *(_sqlSearchInfo.add_sortinfos()) = sortInfo;
}

void SqlSearchInfoCollector::addCalcInfo(const CalcInfo &calcInfo) {
   autil::ScopedLock lock(_mutex);
   *(_sqlSearchInfo.add_calcinfos()) = calcInfo;
}

void SqlSearchInfoCollector::addTvfInfo(const TvfInfo &tvfInfo) {
   autil::ScopedLock lock(_mutex);
   *(_sqlSearchInfo.add_tvfinfos()) = tvfInfo;
}

void SqlSearchInfoCollector::addExchangeInfo(const ExchangeInfo &exchangeInfo) {
   autil::ScopedLock lock(_mutex);
   *(_sqlSearchInfo.add_exchangeinfos()) = exchangeInfo;
}

SqlSearchInfo SqlSearchInfoCollector::getSqlSearchInfo() {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo;
}

void SqlSearchInfoCollector::mergeInSearch() {
    autil::ScopedLock lock(_mutex);
    if (_forbitMerge) {
        return;
    }
    mergeScanInfo();
    mergeJoinInfo();
    mergeAggInfo();
    mergeSortInfo();
    mergeCalcInfo();
    mergeTvfInfo();
}

void SqlSearchInfoCollector::mergeInQrs() {
    autil::ScopedLock lock(_mutex);
    if (_forbitMerge) {
        return;
    }
    mergeScanInfo();
    mergeJoinInfo();
    mergeAggInfo();
    mergeSortInfo();
    mergeCalcInfo();
    mergeTvfInfo();
}

void SqlSearchInfoCollector::mergeSqlSearchInfo(const SqlSearchInfo &sqlSearchInfo) {
    autil::ScopedLock lock(_mutex);
    _sqlSearchInfo.MergeFrom(sqlSearchInfo);
}

void SqlSearchInfoCollector::serializeToString(std::string &searchInfoStr) {
    autil::ScopedLock lock(_mutex);
    _sqlSearchInfo.SerializeToString(&searchInfoStr);
}

bool SqlSearchInfoCollector::deserializeFromString(const std::string &searchInfoStr) {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo.ParseFromString(searchInfoStr);
}

ScanInfo operator + (const ScanInfo &lhs, const ScanInfo &rhs) {
    ScanInfo scanInfo;
    scanInfo.set_kernelname(lhs.kernelname());
    scanInfo.set_tablename(lhs.tablename());
    scanInfo.set_hashkey(lhs.hashkey());
    scanInfo.set_parallelnum(lhs.parallelnum());
    scanInfo.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    scanInfo.set_totalscancount(lhs.totalscancount() + rhs.totalscancount());
    scanInfo.set_totalseekcount(lhs.totalseekcount() + rhs.totalseekcount());
    scanInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    scanInfo.set_totalseektime(lhs.totalseektime() + rhs.totalseektime());
    scanInfo.set_totalevaluatetime(lhs.totalevaluatetime() + rhs.totalevaluatetime());
    scanInfo.set_totaloutputtime(lhs.totaloutputtime() + rhs.totaloutputtime());
    scanInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    scanInfo.set_totalinittime(lhs.totalinittime() + rhs.totalinittime());
    return scanInfo;
}

JoinInfo operator + (const JoinInfo &lhs, const JoinInfo &rhs) {
    JoinInfo joinInfo;
    joinInfo.set_kernelname(lhs.kernelname());
    joinInfo.set_hashkey(lhs.hashkey());
    joinInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    joinInfo.set_hashmapsize(lhs.hashmapsize() + rhs.hashmapsize());
    joinInfo.set_totallefthashcount(lhs.totallefthashcount() + rhs.totallefthashcount());
    joinInfo.set_totalrighthashcount(lhs.totalrighthashcount() + rhs.totalrighthashcount());
    joinInfo.set_totaljoincount(lhs.totaljoincount() + rhs.totaljoincount());
    joinInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    joinInfo.set_totalhashtime(lhs.totalhashtime() + rhs.totalhashtime());
    joinInfo.set_totaljointime(lhs.totaljointime() + rhs.totaljointime());
    joinInfo.set_totalevaluatetime(lhs.totalevaluatetime() + rhs.totalevaluatetime());
    return joinInfo;
}

AggInfo operator + (const AggInfo &lhs, const AggInfo &rhs) {
    AggInfo aggInfo;
    aggInfo.set_kernelname(lhs.kernelname());
    aggInfo.set_hashkey(lhs.hashkey());
    aggInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    aggInfo.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    aggInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    aggInfo.set_collecttime(lhs.collecttime() + rhs.collecttime());
    aggInfo.set_outputacctime(lhs.outputacctime() + rhs.outputacctime());
    aggInfo.set_mergetime(lhs.mergetime() + rhs.mergetime());
    aggInfo.set_outputresulttime(lhs.outputresulttime() + rhs.outputresulttime());
    aggInfo.set_aggpoolsize(lhs.aggpoolsize() + rhs.aggpoolsize());
    return aggInfo;
}

SortInfo operator + (const SortInfo &lhs, const SortInfo &rhs) {
    SortInfo sortInfo;
    sortInfo.set_kernelname(lhs.kernelname());
    sortInfo.set_hashkey(lhs.hashkey());
    sortInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    sortInfo.set_totalmergetime(lhs.totalmergetime() + rhs.totalmergetime());
    sortInfo.set_totaltopktime(lhs.totaltopktime() + rhs.totaltopktime());
    sortInfo.set_totalcompacttime(lhs.totalcompacttime() + rhs.totalcompacttime());
    sortInfo.set_totaloutputtime(lhs.totaloutputtime() + rhs.totaloutputtime());
    sortInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    return sortInfo;
}

CalcInfo operator + (const CalcInfo &lhs, const CalcInfo &rhs) {
    CalcInfo calcInfo;
    calcInfo.set_kernelname(lhs.kernelname());
    calcInfo.set_hashkey(lhs.hashkey());
    calcInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    calcInfo.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    calcInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    return calcInfo;
}

TvfInfo operator + (const TvfInfo &lhs, const TvfInfo &rhs) {
    TvfInfo tvfInfo;
    tvfInfo.set_kernelname(lhs.kernelname());
    tvfInfo.set_hashkey(lhs.hashkey());
    tvfInfo.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    tvfInfo.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    tvfInfo.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    return tvfInfo;
}

void SqlSearchInfoCollector::mergeScanInfo() {
    map<uint32_t, ScanInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.scaninfos_size(); i++) {
        auto scanInfo = _sqlSearchInfo.scaninfos(i);
        uint32_t key = scanInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = scanInfo;
        } else {
            mergeMap[key] = mergeMap[key] + scanInfo;
        }
    }
    _sqlSearchInfo.clear_scaninfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_scaninfos() = iter.second;
    }
}

void SqlSearchInfoCollector::mergeJoinInfo() {
    map<uint32_t, JoinInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.joininfos_size(); i++) {
        auto joinInfo = _sqlSearchInfo.joininfos(i);
        uint32_t key = joinInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = joinInfo;
        } else {
            mergeMap[key] = mergeMap[key] + joinInfo;
        }
    }
    _sqlSearchInfo.clear_joininfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_joininfos() = iter.second;
    }
}

void SqlSearchInfoCollector::mergeAggInfo() {
    map<uint32_t, AggInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.agginfos_size(); i++) {
        auto aggInfo = _sqlSearchInfo.agginfos(i);
        uint32_t key = aggInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = aggInfo;
        } else {
            mergeMap[key] = mergeMap[key] + aggInfo;
        }
    }
    _sqlSearchInfo.clear_agginfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_agginfos() = iter.second;
    }
}

void SqlSearchInfoCollector::mergeSortInfo() {
    map<uint32_t, SortInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.sortinfos_size(); i++) {
        auto sortInfo = _sqlSearchInfo.sortinfos(i);
        uint32_t key = sortInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = sortInfo;
        } else {
            mergeMap[key] = mergeMap[key] + sortInfo;
        }
    }
    _sqlSearchInfo.clear_sortinfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_sortinfos() = iter.second;
    }
}

void SqlSearchInfoCollector::mergeCalcInfo() {
    map<uint32_t, CalcInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.calcinfos_size(); i++) {
        auto calcInfo = _sqlSearchInfo.calcinfos(i);
        uint32_t key = calcInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = calcInfo;
        } else {
            mergeMap[key] = mergeMap[key] + calcInfo;
        }
    }
    _sqlSearchInfo.clear_calcinfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_calcinfos() = iter.second;
    }
}

void SqlSearchInfoCollector::mergeTvfInfo() {
    map<uint32_t, TvfInfo> mergeMap;
    for (int i = 0; i < _sqlSearchInfo.tvfinfos_size(); i++) {
        auto tvfInfo = _sqlSearchInfo.tvfinfos(i);
        uint32_t key = tvfInfo.hashkey();
        auto iter = mergeMap.find(key);
        if (iter == mergeMap.end()) {
            mergeMap[key] = tvfInfo;
        } else {
            mergeMap[key] = mergeMap[key] + tvfInfo;
        }
    }
    _sqlSearchInfo.clear_tvfinfos();
    for (auto iter : mergeMap) {
        *_sqlSearchInfo.add_tvfinfos() = iter.second;
    }
}



END_HA3_NAMESPACE(sql);
