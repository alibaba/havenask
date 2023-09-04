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
#ifndef ISEARCH_BS_INDEXINFOCOLLECTOR_H
#define ISEARCH_BS_INDEXINFOCOLLECTOR_H

#include "autil/LoopThread.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/GenerationKeeper.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "kmonitor_adapter/Monitor.h"
#include "worker_framework/ZkState.h"

namespace build_service { namespace admin {

class IndexInfoCollector
{
public:
    class IndexInfo : public autil::legacy::Jsonizable
    {
    public:
        IndexInfo();
        IndexInfo(const std::string& indexRoot, std::vector<std::string>& clusterNames, int64_t indexSize);

        ~IndexInfo();
        bool operator==(const IndexInfo& other) const;
        void initMetricTags(const proto::BuildId& buildId);

    public:
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    private:
        std::string _indexRoot;
        std::vector<std::string> _clusters;
        int64_t _indexSize;
        bool _isStopped;
        int64_t _stopTs;
        kmonitor::MetricsTags _tags;

    private:
        friend class IndexInfoCollector;
    };

public:
    IndexInfoCollector(cm_basic::ZkWrapper* zkWrapper, const std::string& zkRoot);
    virtual ~IndexInfoCollector();

private:
    IndexInfoCollector(const IndexInfoCollector&);
    IndexInfoCollector& operator=(const IndexInfoCollector&);

public:
    virtual bool init(kmonitor_adapter::Monitor* monitor);
    void update(const proto::BuildId& buildId, GenerationKeeperPtr& gKeeper);
    void stop(const proto::BuildId& buildId);

private:
    void insert(const proto::BuildId& buildId, const IndexInfo& indexInfo);
    void update(const proto::BuildId& buildId, int64_t indexSize);
    bool has(const proto::BuildId& buildId);
    bool recover();

private:
    void report();
    void clear();
    // virtual for test
    virtual void commit();

private:
    mutable autil::ThreadMutex _mapLock;
    cm_basic::ZkWrapper* _zkWrapper;
    std::string _zkRoot;
    worker_framework::ZkState _zkStatus;
    volatile int64_t _zkStatusFileSize;

    std::map<proto::BuildId, IndexInfo> _indexInfos;

    autil::LoopThreadPtr _reportThread;
    autil::LoopThreadPtr _clearThread;

    kmonitor_adapter::MetricPtr _indexSizeMetric;
    kmonitor_adapter::MetricPtr _stoppedIndexSizeMetric;
    kmonitor_adapter::MetricPtr _reportLatencyMetric;
    kmonitor_adapter::MetricPtr _clearLatencyMetric;
    kmonitor_adapter::MetricPtr _unknownIndexSizeCountMetric;
    kmonitor_adapter::MetricPtr _totalIndexCountMetric;
    kmonitor_adapter::MetricPtr _indexInfoFileSizeMetric;

private:
    static int DEFAULT_REPORT_INTERVAL; // second
    static int DEFAULT_CLEAR_INTERVAL;  // second
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(IndexInfoCollector);

}} // namespace build_service::admin

#endif // ISEARCH_BS_INDEXINFOCOLLECTOR_H
