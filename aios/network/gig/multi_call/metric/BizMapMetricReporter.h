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
#ifndef ISEARCH_MULTI_CALL_BIZMAPMETRICREPORTER_H
#define ISEARCH_MULTI_CALL_BIZMAPMETRICREPORTER_H

#include "aios/network/gig/multi_call/metric/BizMetricReporter.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"

namespace multi_call {

class BizMapMetricReporter {
public:
    BizMapMetricReporter(kmonitor::KMonitor *kMonitor, bool isAgent);
    virtual ~BizMapMetricReporter();

private:
    BizMapMetricReporter(const BizMapMetricReporter &);
    BizMapMetricReporter &operator=(const BizMapMetricReporter &);

public:
    void reportSnapshotInfo(const SnapshotInfoCollector &snapInfo);
    void reportReplyInfo(const ReplyInfoCollector &replyInfo);
    BizMetricReporterPtr getBizMetricReporter(const std::string &bizName);

    typedef std::unordered_map<std::string, BizMetricReporterPtr>
        BizMetricReporterMap;

private:
    kmonitor::KMonitor *_kMonitor;
    bool _isAgent;
    BizMetricReporterMap _bizMetricReporterMap;
    autil::ReadWriteLock _lock;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(BizMapMetricReporter);
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_BIZMAPMETRICREPORTER_H