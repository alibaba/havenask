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
#ifndef _ANET_E2E_QOS_H_
#define _ANET_E2E_QOS_H_
#include <stdint.h>
namespace anet {
/**
 * 流控
 */

class E2eQos {
public:
    E2eQos();
    ~E2eQos();
    int AddGroup(int sk, uint32_t gid, uint64_t jobid = 0, uint32_t insid = 0);
    int RemoveGroup(int sk);

private:
    int (*_qos_join_group)(int sk, unsigned long job, unsigned inst, unsigned group);
    int (*_qos_quit_group)(int sk);
    void *handle;
};

extern E2eQos _ge2eQos;
} // namespace anet

#endif
