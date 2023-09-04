/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2018-04-23 14:23
 * Author Name: beifei
 * Author Email: beifei@taobao.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSTAGSMANAGER_H_
#define KMONITOR_CLIENT_CORE_METRICSTAGSMANAGER_H_

#include <map>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsConfig.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/metric/Metric.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

static const int clear_loop = 100;

class MetricsTagsManager {
public:
    MetricsTagsManager(MetricsTags *tags);
    ~MetricsTagsManager() {}

    MetricsTagsPtr GetMetricsTags(const std::map<std::string, std::string> &tags_map);

    void ClearUselessTags() {
        static const int clear_loop = 100;
        if (clearNum_++ % clear_loop != 0) {
            return;
        }
        clearUselessTags();
    }
    void SetConfig(MetricsConfig *config, bool useConfigTags = true) {
        config_ = config;
        if (tags_ != nullptr) {
            if (useConfigTags) {
                for (auto tag : config_->global_tags()->GetTagsMap()) {
                    tags_->AddTag(tag.first, tag.second);
                }
            }
            for (auto tag : config_->CommonTags()) {
                tags_->AddTag(tag.first, tag.second);
            }
        }
    }

private:
    bool validateTags(const std::map<std::string, std::string> &tags) const;

private:
    autil::ThreadMutex metric_mutex_;
    std::map<uint64_t, MetricsTagsPtr> all_tags_;
    MetricsTags *tags_;
    MetricsConfig *config_;
    int clearNum_;

private:
    void clearUselessTags();

private:
    AUTIL_LOG_DECLARE();
};

TYPEDEF_PTR(MetricsTagsManager);

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSTAGSMANAGER_H_
