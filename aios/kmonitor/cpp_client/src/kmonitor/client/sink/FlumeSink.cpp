/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:00
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/sink/FlumeSink.h"

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/common/Util.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsValue.h"
#include "kmonitor/client/metric/Metric.h"
#include "kmonitor/client/net/AgentClient.h"
#include "kmonitor/client/net/LogFileAgentClient.h"
#include "kmonitor/client/net/thrift/EventBuilder.h"
#include "kmonitor/client/net/thrift/ThriftFlumeEvent.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);
AUTIL_LOG_SETUP(kmonitor, FlumeSink);

using std::map;
using std::string;
using std::stringstream;
using std::vector;

const char *FlumeSink::NAME = "FlumeSink";
const string FlumeSink::AGENT_THRIFT_PORT("4141");

FlumeSink::FlumeSink(const std::string &tenant, const std::string &addr, bool enableLogFileSink)
    : Sink(FlumeSink::NAME), tenant_(tenant), client_(nullptr), queue_(QUEUE_CAPACITY) {
    started_ = false;
    remote_send_cnt_ = 0;

    std::string localAddr = addr;
    if (localAddr.empty()) {
        localAddr = getDefaultSinkAddr();
    }
    client_ = new AgentClient(localAddr, SOCK_TIMEOUT_MS);

    event_buffer_.reset(new BatchFlumeEvent());

    if (enableLogFileSink) {
        AUTIL_LOG(INFO, "enable output metrics to log file");
        log_file_client_ = new LogFileAgentClient();
    } else {
        AUTIL_LOG(INFO, "disable output metrics to log file");
        log_file_client_ = nullptr;
    }
}

FlumeSink::FlumeSink(const std::string &tenant, BaseAgentClient *client, BaseAgentClient *logFileClient)
    : Sink(FlumeSink::NAME), tenant_(tenant), queue_(QUEUE_CAPACITY) {
    started_ = false;
    remote_send_cnt_ = 0;
    client_ = client;
    event_buffer_.reset(new BatchFlumeEvent());
    log_file_client_ = logFileClient;
}

string FlumeSink::getDefaultSinkAddr() const { return Util::GetMachineName() + ":" + AGENT_THRIFT_PORT; }

FlumeSink::~FlumeSink() {
    if (client_ != nullptr) {
        delete client_;
        client_ = nullptr;
    }
    if (log_file_client_ != nullptr) {
        delete log_file_client_;
        log_file_client_ = nullptr;
    }
}

bool FlumeSink::Init() {
    if (!client_->Init()) {
        return false;
    }
    if (log_file_client_ != nullptr && !log_file_client_->Init()) {
        return false;
    }
    return true;
}

void FlumeSink::PutMetrics(MetricsRecord *record) {
    const string &tagstring = record->Tags()->ToString();
    for (const auto &value : record->Values()) {
        stringstream_ << value->Name() << " " << record->Timestamp() << " " << value->Value() << " ";
        stringstream_ << tagstring;
        string eventBody = stringstream_.str();
        ThriftFlumeEvent *event = EventBuilder::Build(eventBody);
        stringstream_.str("");
        stringstream_.clear();
        event->AddHeader(Metric::HEADER_TENANT, tenant_);
        if (value->Headers().size() > 0) {
            event->AddHeaders(value->Headers());
        }
        event_buffer_->AppendFlumeEvent(event);
    }
}

void FlumeSink::Flush() {
    BatchFlumeEventPtr events = event_buffer_;
    if (!events->IsEmpty()) {
        AUTIL_LOG(DEBUG, "flume sink flush %lu metrics", events->Size());
        bool ret = client_->AppendBatch(events);
        if (!ret) {
            client_->Close();
        }
        if (log_file_client_ != nullptr) {
            ret = log_file_client_->AppendBatch(events);
            if (!ret) {
                log_file_client_->Close();
            }
        }
    }
    event_buffer_.reset(new BatchFlumeEvent());
}

END_KMONITOR_NAMESPACE(kmonitor);
