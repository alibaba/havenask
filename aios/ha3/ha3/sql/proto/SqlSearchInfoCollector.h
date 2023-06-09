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
#pragma once

#include <memory>
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/proto/SqlSearchInfo.pb.h"

namespace isearch {
namespace sql {

class SqlSearchInfoCollector {
public:
    SqlSearchInfoCollector();
    ~SqlSearchInfoCollector();

private:
    SqlSearchInfoCollector(const SqlSearchInfoCollector &);
    SqlSearchInfoCollector &operator=(const SqlSearchInfoCollector &);

public:
    void serialize(autil::DataBuffer &dataBuffer) const;
    bool deserialize(autil::DataBuffer &dataBuffer);

public:
    void overwriteScanInfo(const ScanInfo &scanInfo);
    void overwriteJoinInfo(const JoinInfo &joinInfo);
    void overwriteAggInfo(const AggInfo &aggInfo);
    void overwriteSortInfo(const SortInfo &sortInfo);
    void overwriteCalcInfo(const CalcInfo &calcInfo);
    void overwriteTvfInfo(const TvfInfo &tvfInfo);
    void overwriteExchangeInfo(const ExchangeInfo &exchangeInfo);
    void overwriteKhronosDataScanInfo(const KhronosDataScanInfo &khronosDataScanInfo);
    void overwriteTableModifyInfo(const TableModifyInfo &tableModifyInfo);
    void overwriteIdentityInfo(const IdentityInfo &identityInfo);
    void overwriteInvertedInfo(const InvertedIndexSearchInfo &invertedInfo);
    void overwriteDelayDpInfo(const DelayDpInfo &delayDpInfo);

    NaviPerfInfo *addNaviPerfInfo();
    NaviPerfInfo *getFirstNaviPerfInfo();
    void setForbitMerge(bool flag);
    bool getForbitMerge();

    SqlSearchInfo getSqlSearchInfo();
    SqlSearchInfo stealSqlSearchInfo();
    void shrinkOverwrite();

    void serializeToString(std::string &searchInfoStr) const;
    bool deserializeFromString(const std::string &searchInfoStr);
    void mergeSqlSearchInfo(SqlSearchInfo &&sqlSearchInfo);
    void swap(SqlSearchInfo &other);

    void setSqlPlanStartTime(int64_t startTime);
    void setSqlPlanTime(int64_t useTime);
    void setSqlPlan2GraphTime(int64_t useTime);
    void setSqlRunGraphTime(int64_t useTime);
    void setSqlRunForkGraphBeginTime(int64_t startTime);
    void setSqlRunForkGraphEndTime(int64_t endTime);
    void setSqlExecTime(int64_t queueUs, int64_t computeUs);

private:
    void mergeSearchInfos(bool overwriteInfo = true);
    void mergeScanInfo(bool overwriteInfo);
    void mergeJoinInfo(bool overwriteInfo);
    void mergeAggInfo(bool overwriteInfo);
    void mergeSortInfo(bool overwriteInfo);
    void mergeCalcInfo(bool overwriteInfo);
    void mergeTvfInfo(bool overwriteInfo);
    void mergeKhronosDataScanInfo(bool overwriteInfo);
    void mergeIdentityInfo(bool overwriteInfo);
    void mergeInvertedInfo(bool overwriteInfo);

private:
    static constexpr size_t SQL_SEARCHINFO_CHECKSUM_BEGIN = 0xBE000001;
    static constexpr size_t SQL_SEARCHINFO_CHECKSUM_END = 0xED000001;

private:
    mutable autil::ThreadMutex _mutex;
    SqlSearchInfo _sqlSearchInfo;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlSearchInfoCollector> SqlSearchInfoCollectorPtr;
} // namespace sql
} // namespace isearch
