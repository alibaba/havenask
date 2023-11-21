/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "sql/proto/SqlSearchInfoCollector.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <map>
#include <utility>

#include "autil/DataBuffer.h"
#include "autil/Lock.h"
#include "navi/log/NaviLogger.h"
#include "sql/proto/SqlSearchInfo.pb.h"

using namespace std;

namespace sql {
AUTIL_LOG_SETUP(sql, SqlSearchInfoCollector);

#define UPDATE_MERGE_COUNT(lhs, rhs)                                                               \
    { lhs.set_mergecount(lhs.mergecount() + rhs.mergecount() + 1); }

SqlSearchInfoCollector::SqlSearchInfoCollector() {}

SqlSearchInfoCollector::~SqlSearchInfoCollector() {}

NaviPerfInfo *SqlSearchInfoCollector::addNaviPerfInfo() {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo.add_naviperfinfos();
}

NaviPerfInfo *SqlSearchInfoCollector::getFirstNaviPerfInfo() {
    autil::ScopedLock lock(_mutex);
    if (_sqlSearchInfo.naviperfinfos_size() == 0) {
        return nullptr;
    } else {
        return _sqlSearchInfo.mutable_naviperfinfos(0);
    }
}

void SqlSearchInfoCollector::overwriteScanInfo(const ScanInfo &scanInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", scanInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_scaninfos()) = scanInfo;
}

void SqlSearchInfoCollector::overwriteJoinInfo(const JoinInfo &joinInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", joinInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_joininfos()) = joinInfo;
}

void SqlSearchInfoCollector::overwriteAggInfo(const AggInfo &aggInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", aggInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_agginfos()) = aggInfo;
}

void SqlSearchInfoCollector::overwriteSortInfo(const SortInfo &sortInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", sortInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_sortinfos()) = sortInfo;
}

void SqlSearchInfoCollector::overwriteCalcInfo(const CalcInfo &calcInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", calcInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_calcinfos()) = calcInfo;
}

void SqlSearchInfoCollector::overwriteTvfInfo(const TvfInfo &tvfInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", tvfInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_tvfinfos()) = tvfInfo;
}

void SqlSearchInfoCollector::overwriteExchangeInfo(const ExchangeInfo &exchangeInfo) {
    NAVI_KERNEL_LOG(TRACE3, "overwrite info [%s]", exchangeInfo.ShortDebugString().c_str());
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_exchangeinfos()) = exchangeInfo;
}

void SqlSearchInfoCollector::overwriteKhronosDataScanInfo(
    const KhronosDataScanInfo &khronosDataScanInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_khronosdatascaninfos()) = khronosDataScanInfo;
}

void SqlSearchInfoCollector::overwriteTableModifyInfo(const TableModifyInfo &tableModifyInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_tablemodifyinfos()) = tableModifyInfo;
}

void SqlSearchInfoCollector::overwriteIdentityInfo(const IdentityInfo &identityInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_identityinfos()) = identityInfo;
}

void SqlSearchInfoCollector::overwriteInvertedInfo(const InvertedIndexSearchInfo &invertedInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_invertedinfos()) = invertedInfo;
}

void SqlSearchInfoCollector::overwriteDelayDpInfo(const DelayDpInfo &delayDpInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_delaydpinfos()) = delayDpInfo;
}

void SqlSearchInfoCollector::addDegradedInfo(const DegradedInfo &degradedInfo) {
    autil::ScopedLock lock(_mutex);
    *(_sqlSearchInfo.add_degradedinfos()) = degradedInfo;
}

SqlSearchInfo SqlSearchInfoCollector::getSqlSearchInfo() {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo;
}

SqlSearchInfo SqlSearchInfoCollector::stealSqlSearchInfo() {
    autil::ScopedLock lock(_mutex);
    return std::move(_sqlSearchInfo);
}

void SqlSearchInfoCollector::shrinkOverwrite() {
    autil::ScopedLock lock(_mutex);
    mergeSearchInfos(true);
}

void SqlSearchInfoCollector::mergeSearchInfos(bool overwriteInfo) {
    mergeScanInfo(overwriteInfo);
    mergeJoinInfo(overwriteInfo);
    mergeAggInfo(overwriteInfo);
    mergeSortInfo(overwriteInfo);
    mergeCalcInfo(overwriteInfo);
    mergeTvfInfo(overwriteInfo);
    mergeKhronosDataScanInfo(overwriteInfo);
    mergeIdentityInfo(overwriteInfo);
    mergeInvertedInfo(overwriteInfo);
    mergeDegradedInfo();
}

void SqlSearchInfoCollector::mergeSqlSearchInfo(SqlSearchInfo &&sqlSearchInfo) {
    autil::ScopedLock lock(_mutex);
    _sqlSearchInfo.MergeFrom(std::move(sqlSearchInfo));
    mergeSearchInfos(false);
}

void SqlSearchInfoCollector::serializeToString(std::string &searchInfoStr) const {
    autil::ScopedLock lock(_mutex);
    _sqlSearchInfo.SerializeToString(&searchInfoStr);
}

bool SqlSearchInfoCollector::deserializeFromString(const std::string &searchInfoStr) {
    autil::ScopedLock lock(_mutex);
    return _sqlSearchInfo.ParseFromString(searchInfoStr);
}

template <typename T>
static void mergeMutableInfos(T &mutable_infos, bool overwriteInfo) {
    using InfoT = typename T::value_type;
    std::map<uint32_t, InfoT> mergeMap;
    for (int i = 0; i < mutable_infos.size(); ++i) {
        InfoT *info = mutable_infos.Mutable(i);
        uint32_t key = info->hashkey();
        if (overwriteInfo) {
            mergeMap[key] = std::move(*info);
        } else {
            auto iter = mergeMap.find(key);
            if (iter == mergeMap.end()) {
                mergeMap[key] = std::move(*info);
            } else {
                mergeFrom(mergeMap[key], *info);
            }
        }
    }
    mutable_infos.Clear();
    for (auto &pair : mergeMap) {
        *mutable_infos.Add() = std::move(pair.second);
    }
}

static void mergeFrom(ScanInfo &lhs, const ScanInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    lhs.set_totalscancount(lhs.totalscancount() + rhs.totalscancount());
    lhs.set_totalseekedcount(lhs.totalseekedcount() + rhs.totalseekedcount());
    lhs.set_totalwholedoccount(lhs.totalwholedoccount() + rhs.totalwholedoccount());
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totalseektime(lhs.totalseektime() + rhs.totalseektime());
    lhs.set_totalevaluatetime(lhs.totalevaluatetime() + rhs.totalevaluatetime());
    lhs.set_totaloutputtime(lhs.totaloutputtime() + rhs.totaloutputtime());
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_totalinittime(lhs.totalinittime() + rhs.totalinittime());
    lhs.set_durationtime(lhs.durationtime() + rhs.durationtime());
    lhs.set_querypoolsize(lhs.querypoolsize() + rhs.querypoolsize());
    lhs.set_isleader(lhs.isleader() && rhs.isleader());
    lhs.set_buildwatermark(std::min(lhs.buildwatermark(), rhs.buildwatermark()));
    lhs.set_waitwatermarktime(lhs.waitwatermarktime() + rhs.waitwatermarktime());
    lhs.set_degradeddocscount(lhs.degradeddocscount() + rhs.degradeddocscount());
    lhs.set_totalscantime(lhs.totalscantime() + rhs.totalscantime());
    lhs.set_extrainfo(rhs.extrainfo());
}

static void mergeFrom(JoinInfo &lhs, const JoinInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_hashmapsize(lhs.hashmapsize() + rhs.hashmapsize());
    lhs.set_totallefthashcount(lhs.totallefthashcount() + rhs.totallefthashcount());
    lhs.set_totalrighthashcount(lhs.totalrighthashcount() + rhs.totalrighthashcount());
    lhs.set_totaljoincount(lhs.totaljoincount() + rhs.totaljoincount());
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totalhashtime(lhs.totalhashtime() + rhs.totalhashtime());
    lhs.set_totaljointime(lhs.totaljointime() + rhs.totaljointime());
    lhs.set_totalevaluatetime(lhs.totalevaluatetime() + rhs.totalevaluatetime());
    lhs.set_totalleftinputcount(lhs.totalleftinputcount() + rhs.totalleftinputcount());
    lhs.set_totalrightinputcount(lhs.totalrightinputcount() + rhs.totalrightinputcount());
}

static void mergeFrom(AggInfo &lhs, const AggInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_collecttime(lhs.collecttime() + rhs.collecttime());
    lhs.set_outputacctime(lhs.outputacctime() + rhs.outputacctime());
    lhs.set_mergetime(lhs.mergetime() + rhs.mergetime());
    lhs.set_outputresulttime(lhs.outputresulttime() + rhs.outputresulttime());
    lhs.set_aggpoolsize(lhs.aggpoolsize() + rhs.aggpoolsize());
    lhs.set_querypoolsize(lhs.querypoolsize() + rhs.querypoolsize());
    lhs.set_totalinputcount(lhs.totalinputcount() + rhs.totalinputcount());
}

static void mergeFrom(SortInfo &lhs, const SortInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totalmergetime(lhs.totalmergetime() + rhs.totalmergetime());
    lhs.set_totaltopktime(lhs.totaltopktime() + rhs.totaltopktime());
    lhs.set_totalcompacttime(lhs.totalcompacttime() + rhs.totalcompacttime());
    lhs.set_totaloutputtime(lhs.totaloutputtime() + rhs.totaloutputtime());
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_totalinputcount(lhs.totalinputcount() + rhs.totalinputcount());
}

static void mergeFrom(CalcInfo &lhs, const CalcInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_totalinputcount(lhs.totalinputcount() + rhs.totalinputcount());
}

static void mergeFrom(TvfInfo &lhs, const TvfInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_totalusetime(lhs.totalusetime() + rhs.totalusetime());
    lhs.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
    lhs.set_totalcomputetimes(lhs.totalcomputetimes() + rhs.totalcomputetimes());
    lhs.set_totalinputcount(lhs.totalinputcount() + rhs.totalinputcount());
}

static void mergeFrom(KhronosDataScanInfo &lhs, const KhronosDataScanInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_callnextcount(lhs.callnextcount() + rhs.callnextcount());
    lhs.set_pointscancount(lhs.pointscancount() + rhs.pointscancount());
    lhs.set_seriesoutputcount(lhs.seriesoutputcount() + rhs.seriesoutputcount());
    lhs.set_lookuptime(lhs.lookuptime() + rhs.lookuptime());
    lhs.set_querypoolsize(lhs.querypoolsize() + rhs.querypoolsize());
    lhs.set_asynciteratorcount(lhs.asynciteratorcount() + rhs.asynciteratorcount());
    lhs.set_iocount(lhs.iocount() + rhs.iocount());
    lhs.set_iosize(lhs.iosize() + rhs.iosize());
    lhs.set_iotokencount(lhs.iotokencount() + rhs.iotokencount());
    lhs.set_iosegmentcount(lhs.iosegmentcount() + rhs.iosegmentcount());
    lhs.set_ioblockcachehitcount(lhs.ioblockcachehitcount() + rhs.ioblockcachehitcount());
    lhs.set_ioblockcachemisscount(lhs.ioblockcachemisscount() + rhs.ioblockcachemisscount());
    lhs.set_prepareiteratorstime(lhs.prepareiteratorstime() + rhs.prepareiteratorstime());
    lhs.set_outputiteratorstime(lhs.outputiteratorstime() + rhs.outputiteratorstime());
}

static void mergeFrom(IdentityInfo &lhs, const IdentityInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    lhs.set_durationtime(lhs.durationtime() + rhs.durationtime());
    lhs.set_totaloutputcount(lhs.totaloutputcount() + rhs.totaloutputcount());
}

static void mergeFrom(BlockAccessInfo &lhs, const BlockAccessInfo &rhs) {
    lhs.set_blockcachehitcount(lhs.blockcachehitcount() + rhs.blockcachehitcount());
    lhs.set_blockcachemisscount(lhs.blockcachemisscount() + rhs.blockcachemisscount());
    lhs.set_blockcachereadlatency(lhs.blockcachereadlatency() + rhs.blockcachereadlatency());
    lhs.set_blockcacheiocount(lhs.blockcacheiocount() + rhs.blockcacheiocount());
    lhs.set_blockcacheiodatasize(lhs.blockcacheiodatasize() + rhs.blockcacheiodatasize());
}

static void mergeFrom(InvertedIndexSearchInfo &lhs, const InvertedIndexSearchInfo &rhs) {
    UPDATE_MERGE_COUNT(lhs, rhs);
    mergeFrom(*lhs.mutable_dictionaryinfo(), rhs.dictionaryinfo());
    mergeFrom(*lhs.mutable_postinginfo(), rhs.postinginfo());
    lhs.set_searchedsegmentcount(lhs.searchedsegmentcount() + rhs.searchedsegmentcount());
    lhs.set_searchedinmemsegmentcount(lhs.searchedinmemsegmentcount()
                                      + rhs.searchedinmemsegmentcount());
    lhs.set_totaldictlookupcount(lhs.totaldictlookupcount() + rhs.totaldictlookupcount());
    lhs.set_totaldicthitcount(lhs.totaldicthitcount() + rhs.totaldicthitcount());
}

static void mergeFrom(DegradedInfo &lhs, const DegradedInfo &rhs) {
    auto *degradedErrorCodes = lhs.mutable_degradederrorcodes();
    degradedErrorCodes->MergeFrom(rhs.degradederrorcodes());
    sort(degradedErrorCodes->begin(), degradedErrorCodes->end());
    auto iter = unique(degradedErrorCodes->begin(), degradedErrorCodes->end());
    degradedErrorCodes->erase(iter, degradedErrorCodes->end());
}

void SqlSearchInfoCollector::mergeScanInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_scaninfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeJoinInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_joininfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeAggInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_agginfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeSortInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_sortinfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeCalcInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_calcinfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeTvfInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_tvfinfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeKhronosDataScanInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_khronosdatascaninfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeIdentityInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_identityinfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeInvertedInfo(bool overwriteInfo) {
    mergeMutableInfos(*_sqlSearchInfo.mutable_invertedinfos(), overwriteInfo);
}

void SqlSearchInfoCollector::mergeDegradedInfo() {
    mergeMutableInfos(*_sqlSearchInfo.mutable_degradedinfos(), false);
}

void SqlSearchInfoCollector::swap(SqlSearchInfo &other) {
    autil::ScopedLock lock(_mutex);
    other.Swap(&_sqlSearchInfo);
}

void SqlSearchInfoCollector::stealTo(SqlSearchInfoCollector &other) {
    SqlSearchInfo tmp;
    swap(tmp);
    other.swap(tmp);
}

void SqlSearchInfoCollector::setSqlPlanStartTime(int64_t startTime) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlplanstarttime(startTime);
}

void SqlSearchInfoCollector::setSqlPlanTime(int64_t useTime, bool cacheHit) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlplantime(useTime);
    timeInfo->set_sqlplancachehit(cacheHit);
}

void SqlSearchInfoCollector::setSqlPlan2GraphTime(int64_t useTime) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlplan2graphtime(useTime);
}

void SqlSearchInfoCollector::setSqlRunGraphTime(int64_t useTime) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlrungraphtime(useTime);
}
void SqlSearchInfoCollector::setSqlRunForkGraphBeginTime(int64_t startTime) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlrunforkgraphbegintime(startTime);
}
void SqlSearchInfoCollector::setSqlRunForkGraphEndTime(int64_t endTime) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlrunforkgraphendtime(endTime);
}

void SqlSearchInfoCollector::setSqlExecTime(int64_t queueUs, int64_t computeUs) {
    autil::ScopedLock lock(_mutex);
    RunSqlTimeInfo *timeInfo = _sqlSearchInfo.mutable_runsqltimeinfo();
    timeInfo->set_sqlexecqueueus(queueUs);
    timeInfo->set_sqlexeccomputeus(computeUs);
}

void SqlSearchInfoCollector::serialize(autil::DataBuffer &dataBuffer) const {
    autil::ScopedLock lock(_mutex);

    dataBuffer.write(SQL_SEARCHINFO_CHECKSUM_BEGIN);
    size_t len = _sqlSearchInfo.ByteSize();
    NAVI_KERNEL_LOG(SCHEDULE1, "sql search info byte size [%lu]", len);
    dataBuffer.write(len);
    void *data = dataBuffer.writeNoCopy(len);
    if (!_sqlSearchInfo.SerializeToArray(data, len)) {
        assert(false && "sql searchinfo serialize failed");
    }
    dataBuffer.write(SQL_SEARCHINFO_CHECKSUM_END);
}

void SqlSearchInfoCollector::deserialize(autil::DataBuffer &dataBuffer) {
    autil::ScopedLock lock(_mutex);

    size_t checksum;
    dataBuffer.read(checksum);
    if (checksum != SQL_SEARCHINFO_CHECKSUM_BEGIN) {
        SQL_LOG(ERROR,
                "invalid checksum, expect [0x%lx] actual [0x%lx]",
                SQL_SEARCHINFO_CHECKSUM_BEGIN,
                checksum);
        return;
    }
    size_t len;
    dataBuffer.read(len);
    NAVI_KERNEL_LOG(SCHEDULE1, "sql search info byte size [%lu]", len);
    const void *buffer = dataBuffer.readNoCopy(len);
    if (!_sqlSearchInfo.ParseFromArray(buffer, len)) {
        SQL_LOG(ERROR, "sql searchinfo parse from array failed, length [%lu]", len);
        return;
    }

    dataBuffer.read(checksum);
    if (checksum != SQL_SEARCHINFO_CHECKSUM_END) {
        SQL_LOG(ERROR,
                "invalid checksum, expect [0x%lx] actual [0x%lx]",
                SQL_SEARCHINFO_CHECKSUM_END,
                checksum);
    }
}

} // namespace sql
