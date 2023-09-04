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
 *       Filename:  master_apply.h
 *
 *    Description:  leader选举，提供基础函数getMaster, 从zk上获取主的cm_server
 *
 *        Version:  0.1
 *        Created:  2013-03-17 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef __MASTER_ALLAY_H_
#define __MASTER_ALLAY_H_
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Log.h"

namespace cm_basic {

class MasterApply
{
private:
    ZkWrapper _zkWrapper;

public:
    MasterApply(const std::string& zk_host, unsigned int timeout_s) : _zkWrapper(zk_host, timeout_s) {}
    ~MasterApply() {}
    /**
     * @brief   设置zk_host和超时时间
     * @param   zk_host
     * @param   timeout_ms 毫秒
     * @return
     */
    void setParameter(const std::string& zk_host, unsigned int timeout_ms)
    {
        _zkWrapper.setParameter(zk_host, timeout_ms);
    }
    /**
     * @brief 获得master 的ip和port, 如果连接失败需要外部重试
     * @param psz_path:[in] zookeeper管理的一组master/slave的根路径
     * @param rs_spec:[out] master节点的ip:port
     * @return 0成功 -1失败
     */
    int getMaster(const std::string& psz_path, std::string& rs_spec);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
