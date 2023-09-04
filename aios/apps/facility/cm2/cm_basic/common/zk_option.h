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
 *       Filename:  zk_option.h
 *
 *    Description:  cm_basic zk_path define
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

#ifndef CONFIG_ZK_OPTION_H_
#define CONFIG_ZK_OPTION_H_

namespace cm_basic {

#define ZK_LE_PATH_NAME "leader_elector"
#define ZK_BINLOG_PATH_NAME "bin_log"
#define ZK_CASCADE_PATH_NAME "cascade"
#define ZK_CLUSTER_LIST_PATH_NAME "cluster_list"
#define ZK_CONFIGSERVER_PATH_NAME "configserver"

} // namespace cm_basic

#endif
