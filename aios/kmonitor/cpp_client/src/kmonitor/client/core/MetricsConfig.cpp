/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 16:13
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#include <string>
#include <vector>
#include <map>
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsConfig.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;
using std::vector;
using std::map;

static const std::string default_sink = "localhost:4141";

MetricsConfig::MetricsConfig() {
    global_tags_ = new MetricsTags();
    inited_ = false;
    enable_system_ = false;
    sink_period_ = 15;
    sink_queue_capacity_ = 1000;
    sink_address_ = "";
    // todo: replace with real cluster vip
    remote_sink_address_ = "";
    use_common_tag_ = true;
}

MetricsConfig::~MetricsConfig() {
    delete global_tags_;
}

void MetricsConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    if (json.GetMode() == FROM_JSON) {
        map<string, string> default_params;
        json.Jsonize("tenant_name", tenant_name_);
        json.Jsonize("service_name", service_name_);
        bool assigned = true;
        json.Jsonize("assigned", assigned, assigned);
        // ignore sink_address if "assigned" is false for compatibility
        if (assigned) {
            json.Jsonize("sink_address", sink_address_, default_sink);
        }
        json.Jsonize("global_tags", default_params, default_params);
        // 用于异步flume client配置
        json.Jsonize("remote_sink_address", remote_sink_address_, remote_sink_address_);
        json.Jsonize("enable_log_file_sink", enable_log_file_sink_, enable_log_file_sink_);
        json.Jsonize("use_common_tag", use_common_tag_, use_common_tag_);
        map<string, string>::iterator iter = default_params.begin();
        for (; iter != default_params.end(); ++iter) {
            AddGlobalTag(iter->first, iter->second);
        }
        inited_ = true;
    } else if (json.GetMode() == TO_JSON) {
        json.Jsonize("tenant_name", tenant_name_);
        json.Jsonize("service_name", service_name_);
        json.Jsonize("sink_address", sink_address_);
        json.Jsonize("remote_sink_address", remote_sink_address_);
        json.Jsonize("enable_log_file_sink", enable_log_file_sink_);
        json.Jsonize("use_common_tag", use_common_tag_, use_common_tag_);
        map<string, string> params = global_tags_->GetTagsMap();
        json.Jsonize("global_tags", params);
    }
}

MetricsConfig& MetricsConfig::operator=(const MetricsConfig &config) {
    if (this != &config) {
        inited_ = config.inited();
        tenant_name_ = config.tenant_name();
        service_name_ = config.service_name();
        sink_address_ = config.sink_address();
        remote_sink_address_ = config.remote_sink_address();
        enable_log_file_sink_ = config.enable_log_file_sink();
        *global_tags_ = *config.global_tags();
        use_common_tag_ = config.use_common_tag();
    }
    return *this;
}

bool MetricsConfig::inited() const {
    return inited_;
}

void MetricsConfig::set_inited(bool flag) {
    inited_ = flag;
}

const string& MetricsConfig::service_name() const {
    return service_name_;
}

void MetricsConfig::set_service_name(const string& service_name) {
    service_name_ = service_name;
}

const string& MetricsConfig::tenant_name() const {
    return tenant_name_;
}

void MetricsConfig::set_tenant_name(const string& tenant_name) {
    tenant_name_ = tenant_name;
}

const string& MetricsConfig::sink_address() const {
    return sink_address_;
}

void MetricsConfig::set_sink_address(const string& sink_address) {
    sink_address_ = sink_address;
}

const string& MetricsConfig::remote_sink_address() const {
    return remote_sink_address_;
}

void MetricsConfig::set_remote_sink_address(const string& remote_sink_address) {
    remote_sink_address_ = remote_sink_address;
}

bool MetricsConfig::enable_log_file_sink() const {
    return enable_log_file_sink_;
}

void MetricsConfig::set_enable_log_file_sink(bool enable_log_file_sink) {
    enable_log_file_sink_ = enable_log_file_sink;
}

bool MetricsConfig::use_common_tag() const {
    return use_common_tag_;
}

void MetricsConfig::set_use_common_tag(bool flag) {
    use_common_tag_ = flag;
}

void MetricsConfig::AddGlobalTag(const string &k, const string &v) {
    global_tags_->AddTag(k, v);
}

void MetricsConfig::AddCommonTag(const string &k, const string &v) {
    common_tags_.emplace(k, v);
}

const MetricsTags* MetricsConfig::global_tags() const {
    return global_tags_;
}

const std::map<std::string, std::string>& MetricsConfig::CommonTags() const {
    return common_tags_;
}

END_KMONITOR_NAMESPACE(kmonitor);
