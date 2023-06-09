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
#include "aios/network/gig/multi_call/java/GigJavaUtil.h"
#include "aios/network/gig/multi_call/java/GigArpcGenerator.h"
#include "aios/network/gig/multi_call/java/GigHttpGenerator.h"
#include "aios/network/gig/multi_call/java/GigTcpGenerator.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GigJavaUtil);

bool GigJavaUtil::convertToTopoNode(const SubProviderInfos &infos,
                                    TopoNodeVec &topoNodeVec) {
    for (int32_t i = 0; i < infos.infos_size(); i++) {
        if (!addTopoNode(infos.infos(i), topoNodeVec)) {
            AUTIL_LOG(ERROR, "invalid pb msg [%s]",
                      infos.infos(i).ShortDebugString().c_str());
            return false;
        }
    }
    return true;
}

bool GigJavaUtil::addTopoNode(const BizProviderInfos &bizProviderInfos,
                              TopoNodeVec &topoNodeVec) {
    for (int32_t i = 0; i < bizProviderInfos.specs_size(); i++) {
        const auto &spec = bizProviderInfos.specs(i);
        TopoNode node;
        node.bizName = bizProviderInfos.biz_name();
        node.partCnt = 1;
        node.partId = 0;
        node.version = bizProviderInfos.version();
        node.weight = spec.weight();
        node.spec.ip = spec.addr();
        node.spec.ports[MC_PROTOCOL_HTTP] = spec.http_port();
        node.spec.ports[MC_PROTOCOL_ARPC] = spec.arpc_port();
        node.spec.ports[MC_PROTOCOL_TCP] = spec.tcp_port();
        node.spec.ports[MC_PROTOCOL_GRPC] = spec.grpc_port();
        node.spec.ports[MC_PROTOCOL_GRPC_STREAM] = spec.grpc_port();
        node.clusterName = bizProviderInfos.cluster_name();
        node.isValid = spec.valid();
        if (!node.generateNodeId()) {
            return false;
        }
        topoNodeVec.push_back(node);
    }
    return true;
}

void GigJavaUtil::convertResourceInfo(
    const QuerySessionPtr &querySession, const std::string &bizName,
    JavaCallback javaCallback, const SearchServiceResourceVector &resourceVec,
    const MetricReporterManagerPtr &metricReporterManger,
    const ReplyInfoCollectorPtr &replyInfoCollector,
    GigSearchResourceInfos &resourceInfos) {
    auto replyInfoCollectorHolder =
        new ReplyInfoCollectorHolder(metricReporterManger, replyInfoCollector);
    auto count = 0;
    auto probeCount = 0;
    auto copyCount = 0;
    int64_t beginTime = autil::TimeUtility::currentTime();
    for (const auto &resource : resourceVec) {
        resource->fillRequestQueryInfo(false, querySession->getTracer(),
                                       querySession->getTraceServerSpan(),
                                       querySession->sessionRequestType());
        resource->setCallBegTime(beginTime);

        auto &info = *resourceInfos.add_infos();
        if (likely(!resource->isCopyRequest())) {
            info.set_resource_ptr(int64_t(new SearchServiceResourceHolder(
                resource, replyInfoCollectorHolder, javaCallback)));
            count++;
            if (resource->isProbeRequest()) {
                probeCount++;
            }
        } else {
            info.set_resource_ptr(0);
            copyCount++;
        }
        info.set_type(GigRequestType(resource->getRequestType()));
        info.set_request_agent_info(
            resource->getRequest()->getAgentQueryInfo());
        auto &specProto = *info.mutable_spec();
        const auto &provider = resource->getProvider(false);
        assert(provider);
        const auto &spec = provider->getSpec();
        specProto.set_addr(spec.ip);
        specProto.set_http_port(spec.ports[MC_PROTOCOL_HTTP]);
        specProto.set_arpc_port(spec.ports[MC_PROTOCOL_ARPC]);
        specProto.set_tcp_port(spec.ports[MC_PROTOCOL_TCP]);
        specProto.set_grpc_port(spec.ports[MC_PROTOCOL_GRPC]);
        specProto.set_weight(provider->getWeight());
    }
    if (0 == count) {
        DELETE_AND_SET_NULL(replyInfoCollectorHolder);
    } else {
        replyInfoCollector->addProbeCallNum(bizName, probeCount);
        replyInfoCollector->addCopyCallNum(bizName, copyCount);
        replyInfoCollectorHolder->setCount(count);
    }
}

void GigJavaUtil::fillGeneratorByPlan(const GigRequestPlan &requestPlan,
                                      GigRequestGenerator *generator) {
    if (requestPlan.has_strategy()) {
        generator->setFlowControlStrategy(requestPlan.strategy());
    }
    if (requestPlan.has_source_id()) {
        generator->setSourceId(requestPlan.source_id());
    }
    if (requestPlan.has_spec()) {
        Spec spec;
        spec.ip = requestPlan.spec().ip();
        int32_t port = (int32_t)requestPlan.spec().port();
        spec.ports[MC_PROTOCOL_TCP] = port;
        spec.ports[MC_PROTOCOL_ARPC] = port;
        spec.ports[MC_PROTOCOL_HTTP] = port;
        spec.ports[MC_PROTOCOL_GRPC] = port;
        spec.ports[MC_PROTOCOL_GRPC_STREAM] = port;
        generator->setSpec(spec);
    }
    if (requestPlan.has_version()) {
        generator->setVersion(requestPlan.version());
    }
    if (requestPlan.has_prefer_version()) {
        generator->setPreferVersion(requestPlan.prefer_version());
    }
    if (requestPlan.has_request_id()) {
        generator->setRequestId(requestPlan.request_id());
    }
    if (requestPlan.has_is_multi()) {
        generator->setMulti(requestPlan.is_multi());
    }
    if (requestPlan.has_disable_retry()) {
        generator->setDisableRetry(requestPlan.disable_retry());
    }
    if (requestPlan.has_disable_probe()) {
        generator->setDisableProbe(requestPlan.disable_probe());
    }
}

void GigJavaUtil::fillSessionByPlan(const GigRequestPlan &plan,
                                    const QuerySessionPtr &session) {
    for (auto &attr : plan.attrs()) {
        if (GIG_TAG_BIZ == attr.name()) {
            session->setBiz(attr.value());
        }
    }
    if (plan.has_trace_context()) {
        auto &traceContext = plan.trace_context();
        session->setEagleeyeUserData(
            traceContext.trace_id(), traceContext.rpc_id(),
            traceContext.user_data(), traceContext.traceparent(),
            traceContext.tracestate());
    }
}

GigRequestGeneratorPtr
GigJavaUtil::genGenerator(QuerySessionPtr &session, const char *body, int len,
                          const GigRequestPlan &requestPlan,
                          const std::shared_ptr<google::protobuf::Arena> &arena) {
    GigRequestGeneratorPtr generator;
    switch (requestPlan.protocol()) {
    case GIG_PROTOCOL_TCP:
        generator.reset(new GigTcpGenerator(requestPlan.cluster_name(),
                        requestPlan.biz_name(), arena));
        break;
    case GIG_PROTOCOL_HTTP:
        generator.reset(new GigHttpGenerator(requestPlan.cluster_name(),
                        requestPlan.biz_name(), arena));
        break;
    case GIG_PROTOCOL_ARPC:
        generator.reset(new GigArpcGenerator(requestPlan.cluster_name(),
                        requestPlan.biz_name(), arena));
        break;
    default:
        AUTIL_LOG(ERROR,
                  "unsupported protocol type[%d] when parse request plan",
                  requestPlan.protocol());
        return GigRequestGeneratorPtr();
    }
    fillGeneratorByPlan(requestPlan, generator.get());
    if (generator->generatePartRequests(body, len, requestPlan)) {
        fillSessionByPlan(requestPlan, session);
        return generator;
    }
    return GigRequestGeneratorPtr();
}

} // namespace multi_call
