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
 *       Filename:  leader_elector.h
 *
 *    Description:  leader选举，提供基础函数, cm_server主从切换时使用
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

#ifndef __CM_BASIC_LEADERELECTOR_H_
#define __CM_BASIC_LEADERELECTOR_H_

#include <functional>
#include <set>
#include <string>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/Log.h"

namespace cm_basic {

class LeaderElectorTest;
class LeaderElector
{
public:
    enum LEStateType { LESTATE_BAD = 0, LESTATE_WAITING, LESTATE_PRIMARY };
    typedef std::function<void(LEStateType)> HandlerFuncType;

public:
    static std::string LEStateToString(LEStateType type);

public:
    LeaderElector(cm_basic::ZkWrapper* p_zk_wrapper);
    ~LeaderElector();

private:
    LeaderElector(const LeaderElector&);
    LeaderElector& operator=(const LeaderElector&);

public:
    /**
     * @brief 设置leader选举的根目录和监听目录
     * @param path:zookeeper leader选举的根目录
     * @param baseName:zookeeper leader选举的节点的前缀名，zookeeper leader node的名字：basename+seqno
     * @return void
     */
    void setParameter(const std::string& path, const std::string& baseName);
    /**
     * @brief
     */
    void setHandler(const HandlerFuncType& handler);

    /**
     * @brief 判断该节点信息是否已经存在
     *
     * @param value:要注册的节点的值，比如:可以把server的ip:tcpport:udpport记录在这
     * @return 成功返回true,失败返回false
     */
    bool checkIfExist(const std::string& value);
    /**
     * @brief 往zookeeper上注册path/basename的序列节点，同时watch该节点，
     *        根据算法设置是master还是slave
     * @param value:要注册的节点的值，比如:可以把server的ip:tcpport:udpport记录在这
     * @return 成功返回true,失败返回false
     */
    bool checkIn(const std::string& value = "");
    /**
     * @brief 给path/_indeedName节点设置数据
     * @param value:数据值
     * @return 设置成功返回true，失败返回false
     */
    bool setValue(const std::string& value);
    /**
     * @brief 关闭zookeeper连接
     * @return
     */
    void shutdown();
    /**
     * @brief 判断当前连接是否失败
     * @return 连接失败返回true，否则返回false
     */
    bool isBad() const { return (LESTATE_BAD == getState()); }
    /**
     * @brief 判断当前自己是否为master
     * @return 是master返回true,否则返回false
     */
    bool isPrimary() const { return (LESTATE_PRIMARY == getState()); }
    /**
     * @brief 判断当前自己是否为slave
     * @return 是slave返回true,否则返回false
     */
    bool isWaiting() const { return (LESTATE_WAITING == getState()); }
    /**
     * @brief 获取节点的状态，是PRIMARY,BAD或WAITING
     * @return 返回节点的状态
     */
    LEStateType getState() const;
    /**
     * @brief 等待节点状态变化。
     * @param state:变化前的状态
     * @param timeout:等待时间，默认为一直等待，直到状态变化
     * @return 返回变化后的节点的状态
     */
    LEStateType waitStateChangeFrom(LEStateType state, int timeout = -1);
    /**
     * @brief 把在zookeeper上创建的节点注销，删除
     * @return true成功，false失败
     */
    bool logOut() { return _pzkWrapper->deleteNode(_path + "/" + _indeedName); }

    std::string getIndeedName() const { return _indeedName; }

public:
    /**
     * @brief 事件ZOO_SESSION_EVENT对应的回调函数, used by callback only
     */
    void connChange(cm_basic::ZkWrapper* zk, const std::string& path, cm_basic::ZkWrapper::ZkStatus status);
    /**
     * @brief 事件ZOO_DELETED_EVENT对应的回调函数
     */
    void existChange(cm_basic::ZkWrapper* zk, const std::string& path, cm_basic::ZkWrapper::ZkStatus status);

private:
    void stateChange(LEStateType state);
    /**
     * @brief 检查当前节点的状态，是为PRIMARY,BAD或WAITING
     * @return 检查成功，返回true，否则返回false
     */
    bool checkPrimary();

private:
    friend class LeaderElectorTest;
    mutable autil::ThreadCond _cond;
    LEStateType _state; ///< 节点的状态

    autil::ThreadMutex _mutex;
    HandlerFuncType _handler;

    cm_basic::ZkWrapper* _pzkWrapper;
    std::string _path;       ///< 用来做leader选举的根目录
    std::string _baseName;   ///< Leader选举的节点的前缀名，zookeeper leader node的名字：basename+seqno
    std::string _indeedName; ///< 由zookeeper生成的真正的节点名, basename+seqno

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif
