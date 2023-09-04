/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:35
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSVALUE_H_
#define KMONITOR_CLIENT_CORE_METRICSVALUE_H_

#include <map>
#include <string>

#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsInfo.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsValue {
public:
    MetricsValue();
    ~MetricsValue();
    MetricsValue(const std::string &name, const std::string &value);
    MetricsValue(const MetricsInfoPtr &info, const std::string &value);
    MetricsValue(const MetricsValue &) = default;
    const std::string &Value() const;
    const std::string &Name() const;
    const std::map<std::string, std::string> &Headers() const;

private:
    MetricsValue &operator=(const MetricsValue &);

private:
    MetricsInfoPtr info_;
    std::string value_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSVALUE_H_
