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
#include "suez/service/WriteTableAccessLog.h"

#include <sstream>

#include "aios/network/arpc/arpc/ANetRPCController.h"
#include "aios/network/gig/multi_call/arpc/ArpcClosure.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "aios/network/gig/multi_call/rpc/GigRpcController.h"
#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(SuezAccess, WriteTableAccessLog);

WriteTableAccessLog::WriteTableAccessLog() : _entryTime(TimeUtility::currentTime()) {}

WriteTableAccessLog::~WriteTableAccessLog() {
    auto payload = StringUtil::toString(*this);
    int64_t exitTime = TimeUtility::currentTime();
    AUTIL_LOG(INFO,
              "%ld->%ld %ldus %s %s {{{ %s }}}",
              _entryTime,
              exitTime,
              exitTime - _entryTime,
              _clientIp.c_str(),
              _traceId.c_str(),
              payload.c_str());
}

void WriteTableAccessLog::collectRPCController(google::protobuf::RpcController *controller) {
    auto gigController = dynamic_cast<multi_call::GigRpcController *>(controller);
    if (!gigController) {
        return;
    }
    auto protocolController = gigController->getProtocolController();
    auto anetRpcController = dynamic_cast<arpc::ANetRPCController *>(protocolController);
    if (anetRpcController) {
        _clientIp = anetRpcController->GetClientAddress();
    }
}

void WriteTableAccessLog::collectRequest(const WriteRequest *request) {
    ostringstream oss;
    oss << "tableName:" << request->tablename();
    oss << ", format:" << request->format();
    oss << ", writeId:[";
    for (const auto &write : request->writes()) {
        oss << write.hashid() << ",";
    }
    oss << "]";
    _queryString = oss.str();
}

void WriteTableAccessLog::collectClosure(google::protobuf::Closure *done) {
    auto arpcClosure = dynamic_cast<multi_call::ArpcClosure *>(done);
    if (arpcClosure) {
        auto querySession = arpcClosure->getQuerySession();
        auto span = querySession->getTraceServerSpan();
        _traceId = opentelemetry::EagleeyeUtil::getETraceId(span);
    }
}

void WriteTableAccessLog::collectResponse(const WriteResponse *response) {
    if (response->has_errorinfo()) {
        auto errorInfo = response->errorinfo();
        _errCode = errorInfo.errorcode();
        _errMsg = errorInfo.errormsg();
    }
}

void WriteTableAccessLog::collectWriteResult(WriteResult writeResult) {
    // TODO:
    // merge multiple param
    _writeResult = std::move(writeResult);
}

std::ostream &operator<<(std::ostream &os, const WriteTableAccessLog &accessLog) {
#define WRITE_FIELDS(key, value, unit) os << key << "=" << (value) << unit << " "
    WRITE_FIELDS("query", "[" + accessLog._queryString + "]", "");
    WRITE_FIELDS("errCode", accessLog._errCode, "");
    WRITE_FIELDS("errMsg", "[" + accessLog._errMsg + "]", "");
    WRITE_FIELDS("inMessageCount", accessLog._writeResult.stat.inMessageCount, "");
    WRITE_FIELDS("parse", accessLog._writeResult.stat.parseLatency, "us");
    WRITE_FIELDS("process", accessLog._writeResult.stat.processLatency, "us");
    WRITE_FIELDS("formatLog", accessLog._writeResult.stat.formatLogLatency, "us");
    WRITE_FIELDS("log", accessLog._writeResult.stat.logLatency, "us");
    WRITE_FIELDS("build", accessLog._writeResult.stat.buildLatency, "us");
    WRITE_FIELDS("buildGap", accessLog._writeResult.watermark.buildGap, "us");
    WRITE_FIELDS("maxCheckpoint", accessLog._writeResult.watermark.maxCp, "");
    WRITE_FIELDS("buildLocator", accessLog._writeResult.watermark.buildLocatorOffset, "");
#undef WRITE_FIELDS
    return os;
}

} // namespace suez
