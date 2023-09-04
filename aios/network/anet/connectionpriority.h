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
#ifndef CONNECTION_PRIOIRITY_H
#define CONNECTION_PRIOIRITY_H

namespace anet {

/* Since we will leverage the SO_PRIORITY socket option, we will need
 * to map our priority to the real priority in linux kernel, that is
 * the output of "/sbin/tc -s qdisc", priority map section . */
typedef enum ConnectionPriority {
    ANET_PRIORITY_DEFAULT = 0,
    ANET_PRIORITY_LOW = 1,
    ANET_PRIORITY_NORMAL = 0,
    ANET_PRIORITY_HIGH = 6
} CONNPRIORITY;

inline bool isValidPriority(CONNPRIORITY prio) { return (prio >= ANET_PRIORITY_DEFAULT && prio <= ANET_PRIORITY_HIGH); }

}; // namespace anet
#endif
