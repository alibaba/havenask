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
/**
 * =====================================================================================
 *
 *       Filename:  common.h
 *    Description:  cm_basic common define
 *
 *        Version:  0.1
 *        Created:  2012-08-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __CM_BASIC_COMMON_H_
#define __CM_BASIC_COMMON_H_

namespace cm_basic {

/* define deletePtr */
#define deletePtr(ptr)                                                                                                 \
    do {                                                                                                               \
        if (ptr) {                                                                                                     \
            delete ptr;                                                                                                \
            ptr = NULL;                                                                                                \
        }                                                                                                              \
    } while (0)

constexpr int IP_LEN = 256;

const char enum_protocol_type[5][8] = {"tcp", "udp", "http", "any", "rdma"}; // 协议类型对应的字符串
const char enum_online_status[3][8] = {"online", "offline", "initing"};
const char enum_node_status[5][16] = {"normal", "abnormal", "timeout", "invalid", "uninit"};
const char enum_topo_type[2][32] = {"one_map_one", "cluster_map_table"};
const char enum_check_type[6][16] = {"heartbeat",   "7level_check", "4level_check",
                                     "keep_online", "keep_offline", "cascade"};
} // namespace cm_basic

#endif
