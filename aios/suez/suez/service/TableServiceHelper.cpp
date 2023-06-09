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
#include "suez/service/TableServiceHelper.h"

#include <string>

#include "aios/network/gig/multi_call/rpc/GigClosure.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
using namespace std;
using namespace kmonitor;

namespace suez {
AUTIL_LOG_SETUP(suez, TableServiceHelper);
void TableServiceHelper::trySetGigEc(google::protobuf::Closure *done, multi_call::MultiCallErrorCode gigEc) {
    auto *gigClosure = dynamic_cast<multi_call::GigClosure *>(done);
    if (gigClosure) {
        gigClosure->setErrorCode(gigEc);
    }
}

void TableServiceHelper::setErrorInfo(ErrorInfo *errInfo,
                                      google::protobuf::Closure *done,
                                      suez::ErrorCode errorCode,
                                      string msg,
                                      const char *queryType,
                                      const string tableName,
                                      kmonitor::MetricsReporter *metricsReporter) {

    errInfo->set_errorcode(errorCode);
    errInfo->set_errormsg(msg);
    AUTIL_LOG(WARN, "%s", string(msg).c_str());
    if (metricsReporter) {
        MetricsTags baseTags = {{{kQueryType, queryType}, {kTableName, tableName}}};
        metricsReporter->report(1, kErrorQps, kmonitor::QPS, &baseTags);
    }
}

} // namespace suez
