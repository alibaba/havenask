/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-24 15:44
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 */

#ifndef KMONITOR_CLIENT_CORE_METRICSRECORD_H_
#define KMONITOR_CLIENT_CORE_METRICSRECORD_H_

#include <map>
#include <string>
#include <vector>

#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MetricsValue.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsRecord {
public:
    MetricsRecord(const MetricsInfoPtr &info, const MetricsTagsPtr &tags, int64_t timestamp);
    ~MetricsRecord();
    MetricsRecord(const MetricsRecord &);
    MetricsRecord &operator=(const MetricsRecord &);

    int64_t Timestamp() const { return timestamp_; }
    const std::string &Name() const;
    const MetricsTagsPtr &Tags() const { return tags_; }
    const std::vector<MetricsValue *> &Values() const { return values_; }
    void AddValue(const MetricsInfoPtr &info, double value);
    void AddValue(const MetricsInfoPtr &info, const std::string &value);

private:
    MetricsInfoPtr info_;
    int64_t timestamp_;
    MetricsTagsPtr tags_;
    std::vector<MetricsValue *> values_;
};

TYPEDEF_PTR(MetricsRecord);

class MetricsRecords {
public:
    MetricsRecords() {}
    ~MetricsRecords() { clear(); }

    MetricsRecords(const MetricsRecords &) = delete;
    MetricsRecords &operator=(const MetricsRecords &) = delete;

    MetricsRecords(MetricsRecords &&other) noexcept;
    MetricsRecords &operator=(MetricsRecords &&other) noexcept;
    void push_back(MetricsRecord *const &record) { records_.push_back(record); }
    size_t size() const { return records_.size(); }
    MetricsRecord *&at(size_t index) { return records_.at(index); }
    MetricsRecord *const &at(size_t index) const { return records_.at(index); }
    void clear();
    const std::vector<MetricsRecord *> &getRecords() const { return records_; }

private:
    std::vector<MetricsRecord *> records_;
};

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_CORE_METRICSRECORD_H_
