/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2020-08-14 15:44
 * Author Name: jianhao
 * Author Email: jianhao@taobao.com
 */

#include "kmonitor/client/core/MetricsValue.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

MetricsValue::MetricsValue() {}

MetricsValue::MetricsValue(const std::string &name, const std::string &value)
    : info_(new MetricsInfo(name, name)), value_(value) {}

MetricsValue::MetricsValue(const MetricsInfoPtr &info, const std::string &value) : info_(info), value_(value) {}

MetricsValue::~MetricsValue() { info_.reset(); }

const std::string &MetricsValue::Value() const { return value_; }

const std::string &MetricsValue::Name() const { return info_->Name(); }

const std::map<std::string, std::string> &MetricsValue::Headers() const { return info_->Headers(); }

END_KMONITOR_NAMESPACE(kmonitor);
