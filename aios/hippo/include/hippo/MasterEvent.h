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
#ifndef HIPPO_MASTEREVENT_H
#define HIPPO_MASTEREVENT_H

#include <functional>
#include "hippo/DriverCommon.h"

namespace hippo {

enum MasterDriverEvent {
    /**
     * The following arguments in EventFunction is always valid:
     *   master
     *   applicationId
     *   event
     */
    EVENT_NO_EVENT = 0,
    
    /**
     * Invoked when the appmanager successfully connected with a leader
     * master. SDKInterface provides master AM functions.
     */
    EVENT_CONNECTED = (1ull << 1),
    
    /**
     * Invoked when the appmanager re-connected with a newly elected master.
     * This is only called when the appmanager has previously been connected.
     */
    EVENT_RECONNECTED = (1ull << 2),
    
    /**
     * Invoked when the appmanager becomes "disconnected" from the master
     * (e.g., the master fails and another is taking over).
     */
    EVENT_DISCONNECTED = (1ull << 3),
    
    /**
     * Invoked when resources have been offered to or reclaimed from this
     * framework. Batch resource changes are informed seperately.
     *
     * Accumulated changes to one resource (e.g. reclaim then alloc) will
     * be merged and only informed according to the final resource. If the
     * final(current) resource is same with last resource informed, the 
     * intermediate changes will also cause an inform without changing
      * any resource.
     * The following arguments in EventFunction is valid:
     *   slotInfo
     */
    EVENT_RESOURCE_NEW_ALLOC = (1ull << 4),
    EVENT_RESOURCE_RECLAIMED = (1ull << 5),
    EVENT_RESOURCE_RECLAIMING = (1ull << 6),
    
    /**
     * Invoked when the resource(_slotResource) of a slot has changed.
     * The following arguments in EventFunction is valid:
     *   slotInfo
     */
    EVENT_SLOT_RESOURCE_CHANGE = (1ull << 7),
    
    /**
     * Invoked when the status(_processStatus, _slaveStatus) of a slot
     * has changed.(e.g., a slave is lost, a worker finishes, a worker
     * core dumps)
     * The following arguments in EventFunction is valid:
     *   slotInfo
     */
    EVENT_SLOT_STATUS_CHANGE = (1ull << 8),
    
    /**
     * ==== deprecated since 0.3.0 ====
     * Invoked when the start command failed, but MasterDriver will
     * retry.(_lastProcessLaunchSuccess, _lastSlotErrorInfo)
     * The following arguments in EventFunction is valid:
     *   slotInfo
     * 
     */
    EVENT_SLOT_START_PROCESS_ERROR = (1ull << 9),
    
    /**
     * ==== deprecated since 0.3.0 ====
     * Invoked when the start command finally failed. MasterDriver has
     * given up the operation.(_processLaunchSuccess, _slotErrorInfo)
     * The following arguments in EventFunction is valid:
     *   slotInfo
     */
    EVENT_SLOT_START_PROCESS_FAILED = (1ull << 10),
    
    /**
     * Invoked when there is an master related error in the AM lib.
     * The following arguments in EventFunction is valid:
     *   message
     */
    EVENT_MASTER_ERROR = (1ull << 11),
    
    /**
     * Invoked when there is an unrecoverable error in the AM lib.
     * AM lib will be aborted BEFORE invoking this callback.
     * The following arguments in EventFunction is valid:
     *   message
     */
    EVENT_FATAL_ERROR = (1ull << 12),
    
    /**
     * AM成为leader(通过zk), 且AM成功连接过hippo master，在等待约定的保护时间后，通知此事件.
     * 注意leader切换时，zk的逻辑会等待sessionTimeout才发生切换
     * 通知此事件后，全量slot列表将通过resource变更消息通知，用户需注意事先清除历史资源记录。
     * 未发送的历史资源申请将被清除。
     */
    EVENT_AM_BECOME_LEADER = (1ull << 13),
    
    /**
     * 在AM当前是leader的情况下，变为follower时（通过zk得知），通知此事件.
     * 当AM不是leader的状态下，所有资源需求无效。
     * SDK不与master进行任何通信，SDK仅提供静态slot列表。
     */
    EVENT_AM_NOLONGER_LEADER = (1ull << 14),
    
    // make it to be uint64_t
    EVENT_INVALID = 0xFFFFFFFFFFFFFFFFull
};

class MasterDriver;

class EventMessage {
public:
    EventMessage() {
        driver = NULL;
        slotInfos = NULL;
    }
    MasterDriver *driver;
    std::string applicationId;
    const std::vector<SlotInfo*> *slotInfos;
    std::string message;
};

typedef std::function<void (MasterDriverEvent, const EventMessage&)
                           > EventFunctionType;


};

#endif //HIPPO_MASTERDRIVER_H

