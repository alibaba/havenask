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
#ifndef CARBON_CARBONMONITOR_H
#define CARBON_CARBONMONITOR_H

#include "carbon/Log.h"
#include "common/common.h"

BEGIN_CARBON_NAMESPACE(monitor);

typedef std::string nodePath_t;
typedef std::string metricName_t;

class CarbonMonitor
{
public:
    CarbonMonitor() {}
    virtual ~CarbonMonitor() {}
private:
    CarbonMonitor(const CarbonMonitor &);
    CarbonMonitor& operator=(const CarbonMonitor &);
public:
    virtual bool init(const std::string &serviceName,
                      const std::string &baseNodePath,
                      uint32_t port) = 0;
    virtual bool start() = 0;
    virtual void stop() = 0;
    virtual void report(const nodePath_t &nodePath,
                        const metricName_t &metricName,
                        double value) = 0;
private:
    CARBON_LOG_DECLARE();
};

static const std::string METRIC_GROUP_SCHEDULE_TIME = "scheduleTime";
static const std::string METRIC_GROUP_TO_STRING_TIME = "toStringTime";
static const std::string METRIC_GROUP_TOTAL_SCHEDULE_TIME = "totalScheduleTime";
static const std::string METRIC_GROUP_PERSIST_FAILED_TIMES = "persistFailedTimes";
static const std::string METRIC_COMPRESS_TIME = "compressTime";
static const std::string METRIC_DECOMPRESS_TIME = "decompressTime";
static const std::string METRIC_WRITE_ZK_FILE_SIZE = "writeZKFileSize";
static const std::string METRIC_WRITE_ZK_TIME = "writeZKTime";
static const std::string METRIC_READ_ZK_FILE_SIZE = "readZKFileSize";
static const std::string METRIC_READ_ZK_TIME = "readZKTime";
static const std::string METRIC_OPERATE_SERVICENODE_FAILED_TIMES = "operateServiceNodeFailedTimes";
static const std::string METRIC_SERVICE_SYNC_TIME = "serviceSyncTime";
static const std::string METRIC_HEALTH_CHECK_TIME = "healthCheckTime";
static const std::string METRIC_ROLE_AVAILABLE_PERCENT = "roleAvailablePercent";
static const std::string METRIC_GROUP_AVAILABLE_PERCENT = "groupAvailablePercent";
static const std::string METRIC_GET_GROUP_STATUS_LATENCY = "getGroupStatusLatency";
static const std::string METRIC_BUFFER_SLOT_COUNT = "bufferSlotCount";
static const std::string METRIC_BUFFER_ALLOC_TIMES = "bufferAllocTimes";
static const std::string METRIC_BUFFER_HEALTH_RATIO = "bufferHealthRatio";
static const std::string METRIC_BUFFER_FEED_SLOT_TIME = "bufferFeedSlotTime";


CARBON_TYPEDEF_PTR(CarbonMonitor);

END_CARBON_NAMESPACE(monitor);

#endif //CARBON_CARBONMONITOR_H
