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
#include "kmonitor/client/sink/PrometheusSink.h"

#include <iostream>
#include <map>
#include <string>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsValue.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, PrometheusSink);

using std::map;
using std::string;
using std::stringstream;

const char *PrometheusSink::NAME = "PrometheusSink";
const string PrometheusSink::PROMETHEUS_GATEWAY_PORT("9091");
const string PrometheusSink::REPORT_PATH("/metrics/job/kmonitor");

const int PrometheusSink::HTTP_CLIENT_THREAD_NUM = 4;
const int PrometheusSink::HTTP_CLIENT_CONNECT_TIMEOUT_MS = 5000;
const int PrometheusSink::HTTP_CLIENT_TIMEOUT_MS = 10000;

PrometheusSink::PrometheusSink(const std::string &addr) : Sink(PrometheusSink::NAME) {
    gatewayAddr_ = addr;

    if (gatewayAddr_.empty()) {
        gatewayAddr_ = getDefaultSinkAddr();
    }

    gatewayAddr_ += "/metrics/job/havenask";

    network::Header httpHeader;
    httpClientPtr_ = std::make_unique<network::HttpClient>(HTTP_CLIENT_THREAD_NUM,
                                                           HTTP_CLIENT_CONNECT_TIMEOUT_MS,
                                                           HTTP_CLIENT_TIMEOUT_MS,
                                                           "prometheus/curl_client",
                                                           nullptr,
                                                           httpHeader);
}

string PrometheusSink::getDefaultSinkAddr() const { return "localhost:" + PROMETHEUS_GATEWAY_PORT + REPORT_PATH; }

PrometheusSink::~PrometheusSink() {}

bool PrometheusSink::Init() { return true; }

void PrometheusSink::PutMetrics(MetricsRecord *record) {
    auto tagMap = record->Tags()->GetTagsMap();
    string tagString = getTagString(tagMap);
    for (const auto &value : record->Values()) {
        // summary minmax value
        std::string metricValue = value->Value();
        if(metricValue.find("_") != std::string::npos) {
            auto summaryMetricValues = autil::StringUtil::split(metricValue, "_");
            if(summaryMetricValues.size() == 0) {
                AUTIL_LOG(WARN, "UnExpect format [metric: %s, value: %s] will skip", record->Name().c_str(), metricValue.c_str());
                continue;
            }
            else {
                metricValue = summaryMetricValues[0];
                float floatValue = 0.0;
                if(!autil::StringUtil::fromString(metricValue, floatValue) || metricValue == "nan" || metricValue == "-nan") {
                    AUTIL_LOG(WARN, "Expect float value, [metric: %s, value: %s] will set as zero", record->Name().c_str(), metricValue.c_str());
                    metricValue = "0";
                }
            }
        }
        // summary json value
        else if(autil::StringUtil::startsWith(metricValue, "{") && autil::StringUtil::endsWith(metricValue, "}")) {
            AUTIL_LOG(WARN, "UnExpect json format, [metric: %s, value: %s] will skip", record->Name().c_str(), metricValue.c_str());
            continue;
        }
        // normal metric
        else {
            float floatValue = 0.0;
            if(!autil::StringUtil::fromString(metricValue, floatValue) || metricValue == "nan" || metricValue == "-nan") {
                AUTIL_LOG(WARN, "Expect float value, [metric: %s, value: %s] will set as zero", record->Name().c_str(), metricValue.c_str());
                metricValue = "0";
            }
        }
        string metric = std::move(value->Name());
        autil::StringUtil::replaceAll(metric, ".", ":");
        autil::StringUtil::replaceAll(metric, "-", "_");
        stringstream_ << metric;
        stringstream_ << tagString;
        stringstream_ << " " << metricValue << "\n";
    }
}

std::string PrometheusSink::getTagString(const std::map<std::string, std::string> &tagMap) {
    std::stringstream ss;
    ss << "{";
    for (auto label = tagMap.begin(); label != tagMap.end(); ++label) {
        if (label != tagMap.begin()) {
            ss << ",";
        }
        ss << label->first << "=\"" << label->second << "\"";
    }
    ss << "}";
    return ss.str();
}

void PrometheusSink::Flush() {
    string sample = stringstream_.str();
    if (!sample.empty()) {
        httpClientPtr_->asyncRequest(
            network::HttpMethod::Post, gatewayAddr_, sample, [this, &sample](const network::HttpResponse &response) {
                if (!response.done || response.curlCode != 0 || response.httpCode != 200) {
                    AUTIL_LOG(WARN,
                              "send requestBody %s to pushgateway failure %s, response is %s, httpCode[%d]",
                              sample.c_str(),
                              gatewayAddr_.c_str(),
                              response.body.c_str(),
                              response.httpCode);
                }
                else {
                    AUTIL_LOG(INFO, "succeed to send sample %s", sample.c_str());
                }
            });

        stringstream_.str("");
        stringstream_.clear();
    }
}

END_KMONITOR_NAMESPACE(kmonitor);
