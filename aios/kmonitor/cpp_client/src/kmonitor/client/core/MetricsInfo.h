/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:35
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSINFO_H_
#define KMONITOR_CLIENT_CORE_METRICSINFO_H_

#include <map>
#include <string>

#include "kmonitor/client/common/Common.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsInfo {
public:
    MetricsInfo(const std::string &name, const std::string &des);
    explicit MetricsInfo(const std::string &name);
    MetricsInfo(const std::string &name, const std::string &dest, const std::map<std::string, std::string> &headers);
    ~MetricsInfo();
    const std::string &Name() const;
    const std::string &Description() const;
    const std::map<std::string, std::string> &Headers() const { return headers_; }

private:
    MetricsInfo(const MetricsInfo &);
    MetricsInfo &operator=(const MetricsInfo &);

private:
    std::string name_;
    std::string description_;
    std::map<std::string, std::string> headers_;
};

typedef std::shared_ptr<MetricsInfo> MetricsInfoPtr;

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSINFO_H_
