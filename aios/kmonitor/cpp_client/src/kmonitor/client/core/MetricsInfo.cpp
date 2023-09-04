/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:35
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#include "kmonitor/client/core/MetricsInfo.h"

#include <stdlib.h>
#include <string>

BEGIN_KMONITOR_NAMESPACE(kmonitor);

using std::string;

MetricsInfo::MetricsInfo(const string &name) : name_(name), description_(name) {}

MetricsInfo::MetricsInfo(const string &name, const string &dest) : name_(name), description_(dest) {}

MetricsInfo::MetricsInfo(const string &name, const string &dest, const std::map<std::string, std::string> &headers)
    : name_(name), description_(dest), headers_(headers) {}

MetricsInfo::~MetricsInfo() {}

const string &MetricsInfo::Name() const { return name_; }

const string &MetricsInfo::Description() const { return description_; }

END_KMONITOR_NAMESPACE(kmonitor);
