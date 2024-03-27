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
#ifndef KMONITOR_CLIENT_SINK_PROMSINK_H_
#define KMONITOR_CLIENT_SINK_PROMSINK_H_

#include <sstream>
#include <string>

#include "aios/network/curl_client/http_client.h"
#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsRecord.h"
#include "kmonitor/client/sink/Sink.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class PrometheusSink : public Sink {
public:
    static const char *NAME;
    explicit PrometheusSink(const std::string &addr);
    ~PrometheusSink() override;
    bool Init() override;
    void PutMetrics(MetricsRecord *record) override;
    void Flush() override;
    bool ReConnect();

private:
    PrometheusSink(const PrometheusSink &);
    PrometheusSink &operator=(const PrometheusSink &);
    std::string getDefaultSinkAddr() const;
    std::string getTagString(const std::map<std::string, std::string> &tagMap);

private:
    std::string gatewayAddr_;
    std::unique_ptr<network::HttpClient> httpClientPtr_;
    std::stringstream stringstream_;

private:
    static const int HTTP_CLIENT_THREAD_NUM;
    static const int HTTP_CLIENT_CONNECT_TIMEOUT_MS;
    static const int HTTP_CLIENT_TIMEOUT_MS;

    static const std::string PROMETHEUS_GATEWAY_PORT;
    static const std::string REPORT_PATH;

private:
    AUTIL_LOG_DECLARE();
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif
