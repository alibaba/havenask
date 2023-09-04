/*
 * Copyright 2014-2020 Alibaba Inc. All rights reserved.
 * Created on: 2017-02-27 11:26
 * Author Name: xsank.mz
 * Author Email: xsank.mz@alibaba-inc.com
 * */

#ifndef KMONITOR_CLIENT_METRIC_METRIC_H_
#define KMONITOR_CLIENT_METRIC_METRIC_H_

#include "autil/Lock.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/common/Common.h"
#include "kmonitor/client/core/MetricsRecord.h"

BEGIN_KMONITOR_NAMESPACE(kmonitor);

class MetricsInfo;
class MetricsRecord;
TYPEDEF_PTR(MetricsInfo);

const int8_t MAX_UNTOUCH_NUM = 20;

class Metric {
public:
    Metric(const std::string &name);
    virtual ~Metric();
    Metric(const Metric &) = delete;

protected:
    virtual void doUpdate(double value) = 0;
    virtual void doSnapshot(MetricsRecord *record, int64_t period /*ms*/) = 0;

public:
    void Update(double value) {
        autil::ScopedLock lock(metric_mutex_);
        doUpdate(value);
        Touch();
    }

    void Snapshot(MetricsRecord *record, int64_t period = DEFAULT_LEVEL_TIME_MS) {
        autil::ScopedLock lock(metric_mutex_);
        if (untouch_num_ >= 0) {
            doSnapshot(record, period);
            Untouch();
        }
    }

    bool canRecycle() {
        autil::ScopedLock lock(metric_mutex_);
        if (ref_cnt_ <= 0 && untouch_num_ > MAX_UNTOUCH_NUM) {
            //连续MAX_UNTOUCH_NUM个Snapshot周期都没有被update过，就认为可回收了
            return true;
        }
        return false;
    }

    void Acquire() {
        autil::ScopedLock lock(metric_mutex_);
        ++ref_cnt_;
    }

    void Release() {
        autil::ScopedLock lock(metric_mutex_);
        --ref_cnt_;
        // Inited metrics can recycle immediately after release.
        // Uninited metric will recycle delay untouch_num_ cycles.
        if (untouch_num_ < 0) {
            Touch();
        }
    }

public:
    void Touch() { untouch_num_ = 0; }

    void Untouch() { ++untouch_num_; }

private:
    Metric &operator=(const Metric &);

private:
    int8_t untouch_num_;
    int32_t ref_cnt_;
    autil::ThreadMutex metric_mutex_;

protected:
    MetricsInfoPtr info_;

public:
    static const std::string HEADER_IP;
    static const std::string HEADER_TENANT;
    static const std::string HEADER_FORMAT;
};

TYPEDEF_PTR(Metric);

END_KMONITOR_NAMESPACE(kmonitor);

#endif // KMONITOR_CLIENT_METRIC_METRIC_H_
