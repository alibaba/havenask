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
#include "navi/engine/ScheduleInfo.h"
#include "navi/proto/GraphVis.pb.h"

namespace navi {

void ScheduleInfo::fillProto(ScheduleInfoDef *info) const {
    info->set_enqueue_time(enqueueTime);
    info->set_dequeue_time(dequeueTime);
    info->set_running_thread(runningThread);
    info->set_sched_tid(schedTid);
    info->set_signal_tid(signalTid);
    info->set_process_tid(processTid);
    info->set_thread_counter(threadCounter);
    info->set_thread_wait_counter(threadWaitCounter);
    info->set_queue_size(queueSize);
}

}
