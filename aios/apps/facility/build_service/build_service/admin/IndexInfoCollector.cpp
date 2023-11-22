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
#include "build_service/admin/IndexInfoCollector.h"

#include <algorithm>
#include <ext/alloc_traits.h>
#include <functional>
#include <memory>
#include <ostream>
#include <unistd.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common/CpuSpeedEstimater.h"
#include "build_service/common/PathDefine.h"
#include "build_service/proto/ProtoComparator.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/Monitor.h"
#include "fslib/util/FileUtil.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor_adapter/Monitor.h"
#include "worker_framework/LeaderElector.h"
#include "worker_framework/WorkerState.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace build_service::proto;
using namespace build_service::util;
using namespace build_service::common;

using namespace worker_framework;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, IndexInfoCollector);

int IndexInfoCollector::DEFAULT_REPORT_INTERVAL = 5;
int IndexInfoCollector::DEFAULT_CLEAR_INTERVAL = 300;

IndexInfoCollector::IndexInfo::IndexInfo() : _indexSize(-1), _isStopped(false), _stopTs(-1) {}

IndexInfoCollector::IndexInfo::IndexInfo(const string& indexRoot, vector<string>& clusterNames, int64_t indexSize)
    : _indexRoot(indexRoot)
    , _clusters(clusterNames)
    , _indexSize(indexSize)
    , _isStopped(false)
    , _stopTs(-1)
{
}

IndexInfoCollector::IndexInfo::~IndexInfo() {}

bool IndexInfoCollector::IndexInfo::operator==(const IndexInfo& other) const
{
    return _indexRoot == other._indexRoot && _indexSize == other._indexSize && _isStopped == other._isStopped &&
           _stopTs == other._stopTs && _clusters == other._clusters;
}

void IndexInfoCollector::IndexInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("indexRoot", _indexRoot, _indexRoot);
    json.Jsonize("indexSize", _indexSize, _indexSize);
    json.Jsonize("isStopped", _isStopped, _isStopped);
    json.Jsonize("stopTs", _stopTs, _stopTs);
    json.Jsonize("clusters", _clusters, _clusters);
}

void IndexInfoCollector::IndexInfo::initMetricTags(const BuildId& buildId)
{
    _tags.AddTag("generation", StringUtil::toString(buildId.generationid()));
    _tags.AddTag("buildAppName", buildId.appname());
    _tags.AddTag("dataTable", buildId.datatable());
}

IndexInfoCollector::IndexInfoCollector(cm_basic::ZkWrapper* zkWrapper, const string& zkRoot)
    : _zkWrapper(zkWrapper)
    , _zkRoot(zkRoot)
    , _zkStatus(zkWrapper, fslib::util::FileUtil::joinFilePath(zkRoot, PathDefine::ZK_GENERATION_INDEX_INFOS))
    , _zkStatusFileSize(0)
{
}

IndexInfoCollector::~IndexInfoCollector() {}

bool IndexInfoCollector::init(kmonitor_adapter::Monitor* monitor)
{
    int reportInterval = autil::EnvUtil::getEnv(BS_ENV_INDEX_INFO_REPORT_INTERVAL, DEFAULT_REPORT_INTERVAL);

    _reportThread =
        LoopThread::createLoopThread(bind(&IndexInfoCollector::report, this),
                                     (reportInterval > 0 ? reportInterval : DEFAULT_REPORT_INTERVAL) * 1000 * 1000);
    if (!_reportThread) {
        BS_LOG(ERROR, "init IndexInfoCollector failed.");
        return false;
    }

    int clearInterval = autil::EnvUtil::getEnv(BS_ENV_INDEX_INFO_CLEAR_INTERVAL, DEFAULT_CLEAR_INTERVAL);
    _clearThread =
        LoopThread::createLoopThread(bind(&IndexInfoCollector::clear, this),
                                     (clearInterval > 0 ? clearInterval : DEFAULT_CLEAR_INTERVAL) * 1000 * 1000);
    if (!_clearThread) {
        BS_LOG(ERROR, "init IndexInfoCollector failed.");
        return false;
    }

    if (!recover()) {
        return false;
    }

    if (!monitor) {
        BS_LOG(INFO, "no metricProvider, will not report IndexSizeMetric");
        return true;
    }

    {
        _indexSizeMetric = monitor->registerGaugeMetric("statistics.indexSize", kmonitor::FATAL);
        _stoppedIndexSizeMetric = monitor->registerGaugeMetric("statistics.stoppedIndexSize", kmonitor::FATAL);
        _unknownIndexSizeCountMetric =
            monitor->registerGaugeMetric("statistics.unknownIndexSizeCount", kmonitor::FATAL);
        _reportLatencyMetric = monitor->registerGaugePercentileMetric("debug.indexInfoReportLatency", kmonitor::FATAL);
        _clearLatencyMetric = monitor->registerGaugePercentileMetric("debug.indexInfoClearLatency", kmonitor::FATAL);
        _totalIndexCountMetric = monitor->registerGaugeMetric("statistics.totalIndexCount", kmonitor::FATAL);
        _indexInfoFileSizeMetric = monitor->registerGaugeMetric("statistics.indexInfoFileSize", kmonitor::FATAL);
    }
    return true;
}

void IndexInfoCollector::insert(const BuildId& buildId, const IndexInfo& indexInfo)
{
    autil::ScopedLock lock(_mapLock);
    _indexInfos.insert(make_pair(buildId, indexInfo));
    BS_LOG(INFO, "add indexSize[%ld] for %s", indexInfo._indexSize, buildId.ShortDebugString().c_str());
    commit();
}

bool IndexInfoCollector::has(const BuildId& buildId)
{
    autil::ScopedLock lock(_mapLock);
    return _indexInfos.find(buildId) != _indexInfos.end();
}

void IndexInfoCollector::update(const BuildId& buildId, int64_t indexSize)
{
    autil::ScopedLock lock(_mapLock);
    auto iter = _indexInfos.find(buildId);
    if (iter == _indexInfos.end()) {
        return;
    }
    if (iter->second._indexSize == indexSize) {
        return;
    }
    BS_LOG(INFO, "update indexSize[%ld] for %s", indexSize, buildId.ShortDebugString().c_str());
    iter->second._indexSize = indexSize;
    commit();
}

void IndexInfoCollector::stop(const BuildId& buildId)
{
    autil::ScopedLock lock(_mapLock);
    auto iter = _indexInfos.find(buildId);
    if (iter == _indexInfos.end()) {
        return;
    }
    if (iter->second._isStopped) {
        return;
    }

    iter->second._isStopped = true;
    iter->second._stopTs = TimeUtility::currentTimeInSeconds();
    commit();
}

void IndexInfoCollector::report()
{
    autil::ScopedLock lock(_mapLock);
    kmonitor_adapter::ScopeLatencyReporter startReport(_reportLatencyMetric);
    int unknownIndexSizeCount = 0;
    for (auto iter = _indexInfos.begin(); iter != _indexInfos.end(); iter++) {
        if (iter->second._indexSize > 0) {
            REPORT_KMONITOR_METRIC2(_indexSizeMetric, iter->second._tags, iter->second._indexSize);
            if (iter->second._isStopped) {
                REPORT_KMONITOR_METRIC2(_stoppedIndexSizeMetric, iter->second._tags, iter->second._indexSize);
            }
        } else {
            unknownIndexSizeCount++;
        }
    }

    REPORT_KMONITOR_METRIC(_unknownIndexSizeCountMetric, unknownIndexSizeCount);
    REPORT_KMONITOR_METRIC(_totalIndexCountMetric, _indexInfos.size());
    REPORT_KMONITOR_METRIC(_indexInfoFileSizeMetric, _zkStatusFileSize);
}

void IndexInfoCollector::clear()
{
    kmonitor_adapter::ScopeLatencyReporter startReport(_clearLatencyMetric);
    map<BuildId, IndexInfo> allIndexInfos;
    {
        autil::ScopedLock lock(_mapLock);
        allIndexInfos = _indexInfos;
    }
    vector<BuildId> toBeClear;
    for (auto iter = allIndexInfos.begin(); iter != allIndexInfos.end(); iter++) {
        if (!iter->second._isStopped) {
            continue;
        }
        bool isIndexExist = false;
        for (auto& cluster : iter->second._clusters) {
            string generationPath = IndexPathConstructor::getGenerationIndexPath(iter->second._indexRoot, cluster,
                                                                                 iter->first.generationid());
            if (!fslib::util::FileUtil::isExist(generationPath, isIndexExist)) {
                // if cant determine whether it was exist.
                isIndexExist = true;
                break;
            }
            if (isIndexExist) {
                break;
            }
        }

        usleep(100000);
        if (!isIndexExist) {
            toBeClear.push_back(iter->first);
        }
    }
    // clear
    {
        autil::ScopedLock lock(_mapLock);
        for (size_t i = 0; i < toBeClear.size(); i++) {
            _indexInfos.erase(toBeClear[i]);
            BS_LOG(INFO, "clear indexInfo for %s", toBeClear[i].ShortDebugString().c_str());
        }
        if (toBeClear.size() == 0) {
            return;
        }
        commit();
    }
}

void IndexInfoCollector::commit()
{
    vector<Any> anyVec;
    anyVec.reserve(_indexInfos.size());
    for (auto iter = _indexInfos.begin(); iter != _indexInfos.end(); iter++) {
        JsonMap jsonMap;
        jsonMap["app"] = ToJson(iter->first.appname());
        jsonMap["generationId"] = ToJson(iter->first.generationid());
        jsonMap["datatable"] = ToJson(iter->first.datatable());
        jsonMap["indexInfo"] = ToJson(iter->second);
        anyVec.push_back(jsonMap);
    }

    string statusStr = ToJsonString(anyVec);
    _zkStatusFileSize = statusStr.size();
    if (WorkerState::EC_FAIL == _zkStatus.write(statusStr)) {
        string errorMsg = "update generationd index infos [" + statusStr + "] failed.";
        BS_LOG(WARN, "%s", errorMsg.c_str());
        return;
    }
    BS_LOG(INFO, "commit IndexInfoCollector[%lu]", _indexInfos.size());
}

bool IndexInfoCollector::recover()
{
    string filePath = fslib::util::FileUtil::joinFilePath(_zkRoot, PathDefine::ZK_GENERATION_INDEX_INFOS);
    bool isExist = false;
    if (!fslib::util::FileUtil::isExist(filePath, isExist)) {
        BS_LOG(ERROR, "recover IndexInfoCollector failed");
        return false;
    }
    if (!isExist) {
        BS_LOG(INFO, "%s is not exist, no need to recover", filePath.c_str());
        return true;
    }

    string content;
    WorkerState::ErrorCode ec = _zkStatus.read(content);
    if (WorkerState::EC_FAIL == ec) {
        stringstream ss;
        ss << "read generation index infos failed, ec[" << int(ec) << "].";
        string errorMsg = ss.str();
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    vector<Any> anyVec;
    FromJsonString(anyVec, content);
    for (size_t i = 0; i < anyVec.size(); i++) {
        JsonMap jsonMap = AnyCast<JsonMap>(anyVec[i]);
        BuildId buildId;
        buildId.set_appname(AnyCast<string>(jsonMap["app"]));
        buildId.set_datatable(AnyCast<string>(jsonMap["datatable"]));
        buildId.set_generationid(JsonNumberCast<uint32_t>(jsonMap["generationId"]));
        IndexInfo indexInfo;
        FromJson(indexInfo, jsonMap["indexInfo"]);
        indexInfo.initMetricTags(buildId);
        _indexInfos.insert(make_pair(buildId, indexInfo));
    }
    BS_LOG(INFO, "recover IndexInfoCollector[%lu]", _indexInfos.size());
    return true;
}

void IndexInfoCollector::update(const BuildId& buildId, GenerationKeeperPtr& gKeeper)
{
    int64_t indexSize = -1;
    gKeeper->getTotalRemainIndexSize(indexSize);
    if (has(buildId)) {
        update(buildId, indexSize);
    } else {
        vector<string> clusters = gKeeper->getClusterNames();
        if (clusters.size() == 0) {
            return;
        }
        IndexInfoCollector::IndexInfo indexInfo(gKeeper->getIndexRoot(), clusters, indexSize);
        indexInfo.initMetricTags(buildId);
        insert(buildId, indexInfo);
    }
}

}} // namespace build_service::admin
