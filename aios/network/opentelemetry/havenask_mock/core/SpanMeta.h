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

#include <map>
#include <memory>
#include <string>

namespace opentelemetry {
// using AttributeValue = std::variant<bool, int32_t, int64_t, uint32_t, double, const std::string&>;
using AttributeValue = std::string;
using AttributeMap = std::map<std::string, AttributeValue>;

enum class SpanKind {
    kInternal,
    kServer,
    kClient,
    kProducer,
    kConsumer,
};

// StatusCode - Represents the canonical set of status codes of a finished Span.
enum class StatusCode {
    kUnset, // default status
    kOk,    // Operation has completed successfully.
    kError  // The operation contains an error
};

constexpr char kEagleeyeServiceName[] = "eagleeye.service_name";
constexpr char kEagleeyeMethodName[] = "eagleeye.method_name";
constexpr char kEagleeyeAppId[] = "eagleeye.app_id";
constexpr char kEagleeyeAppGroup[] = "eagleeye.app_group";
constexpr char kEagleeyeRpcIdKey[] = "eagleeye.rpc_id";
constexpr char kEagleeyeClientRpcType[] = "eagleeye.client_rpc_type";
constexpr char kEagleeyeTraceId[] = "EagleEye-TraceId";
constexpr char kEagleeyeRpcId[] = "EagleEye-RpcId";
constexpr char kEagleeyeUserData[] = "EagleEye-UserData";
constexpr char kEagleeyeClusterTest[] = "EagleEye-ClusterTest";
constexpr char kEagleeyeDefaultSpanId[] = "1111111111111111";
constexpr char kEagleeyeDefaultRpcId[] = "0";
constexpr char kEagleeyeUserDataEntrySeparator = 0x12;
constexpr char kEagleeyeUserDataKVSeparator = 0x14;

extern const std::string EMPTY_STRING;

// otlp
constexpr char kOpenTelemetryTraceParent[] = "traceparent";
constexpr char kOpenTelemetryTraceState[] = "tracestate";
constexpr size_t kTraceParentTraceIdStart = 3;
constexpr size_t kTraceParentTraceIdSize = 32;
constexpr size_t kTraceParentSpanIdStart = 36;
constexpr size_t kTraceParentSpanIdSize = 16;
constexpr size_t kTraceParentFlagsStart = 53;
constexpr size_t kTraceParentFlagsSize = 2;
constexpr size_t kTraceParentSize = 55;
constexpr char kTraceStateKeyValueSeparator = '=';
constexpr char kTraceStateMembersSeparator = ',';
constexpr char kTraceParentSeparator = '-';

constexpr char kTraceStateLogLevel[] = "level";

constexpr char kSpanAttrReqContentLength[] = "request_content_length.bytes";
constexpr char kSpanAttrRespContentLength[] = "response_content_length.bytes";
constexpr char kSpanAttrNetPeerIp[] = "net.peer.ip";
constexpr char kSpanAttrNetHostIp[] = "net.host.ip";

constexpr char kEnvPlatform[] = "PLATFORM";
constexpr char kEnvHippoCluster[] = "HIPPO_SERVICE_NAME";
constexpr char kEnvHippoApp[] = "HIPPO_APP";
constexpr char kEnvC2Group[] = "C2_GROUP";
constexpr char kEnvC2Role[] = "C2_ROLE";
constexpr char kEnvRole[] = "role";
constexpr char kEnvHippoVMIP[] = "HIPPO_ENV_VM_IP";
constexpr char kHostSeparator[] = ".";

constexpr size_t kMaxRequestEventSize = 4 * 1024 * 1024;
constexpr size_t kMaxResponseEventSize = 8 * 1024 * 1024;

#define OTEL_TYPEDEF_PTR(x) typedef std::shared_ptr<x> x##Ptr;
#define OTEL_TYPEDEF_CONST_PTR(x) typedef std::shared_ptr<const x> x##ConstPtr;

} // namespace opentelemetry
