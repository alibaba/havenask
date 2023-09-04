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
#ifndef ISEARCH_MULTI_CALL_GIGJAVAUTIL_H
#define ISEARCH_MULTI_CALL_GIGJAVAUTIL_H

#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/java/GigApi.h"
#include "aios/network/gig/multi_call/java/GigRequestGenerator.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/proto/GigCallProto.pb.h"
#include "aios/network/gig/multi_call/proto/GigSelectProto.pb.h"
#include "aios/network/gig/multi_call/service/ReplicaController.h"
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"

namespace multi_call {

struct ReplyInfoCollectorHolder {
public:
    ReplyInfoCollectorHolder(const MetricReporterManagerPtr &metricReporterManger,
                             const ReplyInfoCollectorPtr &replyInfoCollector)
        : _metricReporterManager(metricReporterManger)
        , _replyInfoCollector(replyInfoCollector) {
        atomic_set(&_count, 0);
    }
    void setCount(int64_t count) {
        atomic_set(&_count, count);
    }
    void decCount(const std::string &bizName, const SearchServiceReplicaPtr &replica) {
        assert(replica);
        if (0 == atomic_dec_return(&_count)) {
            _metricReporterManager->reportReplyInfo(*_replyInfoCollector);
            const auto &controller = *replica->getReplicaController();
            float baseAvgLatency = 0.0f;
            float baseErrorRatio = 0.0f;
            float baseDegradeRatio = 0.0f;
            auto bizReporterPtr = _metricReporterManager->getBizMetricReporter(bizName);
            if (bizReporterPtr) {
                if (controller.getBaseAvgLatency(baseAvgLatency)) {
                    bizReporterPtr->reportSnapshotMinAvgLatency(baseAvgLatency);
                }
                if (controller.getBaseErrorRatio(baseErrorRatio)) {
                    bizReporterPtr->reportSnapshotMinErrorRatio(baseErrorRatio);
                }
                if (controller.getBaseDegradeRatio(baseDegradeRatio)) {
                    bizReporterPtr->reportSnapshotMinDegradeRatio(baseDegradeRatio);
                }
                bizReporterPtr->reportReplicaAvgWeight(controller.getAvgWeight());
            }
            delete this;
        }
    }
    const ReplyInfoCollectorPtr &getReplyInfoCollector() const {
        return _replyInfoCollector;
    }

private:
    atomic64_t _count;
    MetricReporterManagerPtr _metricReporterManager;
    ReplyInfoCollectorPtr _replyInfoCollector;
};

struct SearchServiceResourceHolder {
public:
    SearchServiceResourceHolder(const SearchServiceResourcePtr &ptr_,
                                ReplyInfoCollectorHolder *replyInfoCollectorHolder_,
                                JavaCallback javaCallback_)
        : ptr(ptr_)
        , replyInfoCollectorHolder(replyInfoCollectorHolder_)
        , javaCallback(javaCallback_) {
    }
    SearchServiceResourcePtr ptr;
    ReplyInfoCollectorHolder *replyInfoCollectorHolder;
    JavaCallback javaCallback;
};

struct QueryInfoHolder {
public:
    QueryInfoHolder(const QueryInfoPtr &ptr_, JavaCallback javaCallback_)
        : ptr(ptr_)
        , javaCallback(javaCallback_) {
    }
    QueryInfoPtr ptr;
    JavaCallback javaCallback;
};

class GigJavaUtil
{
public:
    static bool convertToTopoNode(const SubProviderInfos &infos, TopoNodeVec &topoNodeVec);
    static void convertResourceInfo(const QuerySessionPtr &querySession, const std::string &bizName,
                                    JavaCallback javaCallback,
                                    const SearchServiceResourceVector &resourceVec,
                                    const MetricReporterManagerPtr &metricReporterManger,
                                    const ReplyInfoCollectorPtr &replyInfoCollector,
                                    GigSearchResourceInfos &resourceInfos);
    static void fillGeneratorByPlan(const GigRequestPlan &requestPlan,
                                    GigRequestGenerator *generator);
    static void fillSessionByPlan(const GigRequestPlan &requestPlan,
                                  const QuerySessionPtr &generator);
    static GigRequestGeneratorPtr
    genGenerator(QuerySessionPtr &session, const char *body, int len,
                 const GigRequestPlan &requestPlan,
                 const std::shared_ptr<google::protobuf::Arena> &arena);

private:
    static bool addTopoNode(const BizProviderInfos &bizProviderInfos, TopoNodeVec &topoNodeVec);

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigJavaUtil);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGJAVAUTIL_H
