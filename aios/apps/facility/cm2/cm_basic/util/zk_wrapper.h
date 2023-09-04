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
 *       Filename:  zk_wrapper.h
 *
 *    Description:  提供基础zk客户端函数
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

#ifndef __CM_BASIC_ZKWRAPPER_H_
#define __CM_BASIC_ZKWRAPPER_H_

#include <functional>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "zookeeper/zookeeper.h"

#if 1
#ifdef __cplusplus
extern "C" {
#endif
ZOOAPI void log_message(ZooLogLevel curLevel, int line, const char* funcName, const char* message);

#ifdef __cplusplus
}
#endif
#endif

namespace cm_basic {

class ZkWrapper
{
private:
    ZkWrapper(const ZkWrapper&);
    ZkWrapper& operator=(const ZkWrapper&);

public:
    enum ZkStatus { ZK_BAD = 0, ZK_CONNECTING, ZK_CONNECTED };
    typedef std::function<void(ZkWrapper*, const std::string&, ZkStatus)> CallbackFuncType;
    /**
     * @brief 构造函数
     * @param host: zookeeper server的ip:port
     * @param timeout: zookeeper的超时时间（秒/毫秒），取决于参数 isMillisecond
     * @param isMillisecond: true ：时间单位为ms，false ：时间单位为秒。
     * @return
     */
    ZkWrapper(const std::string& host = "", unsigned int timeout = 10, bool isMillisecond = false);

    virtual ~ZkWrapper();
    /**
     * @brief   leader选举策略，判断psz_child节点是否为leader
     * @param   rvec_nodes:[in] psz_path节点下的所有孩子节点
     * @param   psz_path:[in] leader选举的zookeeper中的根路径
     * @param   psz_child:[in] 需要判断是否为master的节点名
     * @param   b_primary:[out] 在函数成功返回后，如果psz_child节点为master则为true，如果为slave则为false
     * @return  0成功，-1失败
     */
    int leaderElectorStrategy(std::vector<std::string>& rvec_nodes, const std::string& psz_path,
                              const std::string& psz_child, bool& b_primary);
    /**
     * @brief 设置zookeeper server的ip和port，以及超时时间
     * @param host,zookeeper server的ip:port
     * @param timeout ,zookeeper的超时时间
     * @return
     */
    void setParameter(const std::string& host = "", unsigned int timeout_ms = 10000);
    /**
     * @brief 连接zookeeper集群，得到和zookeeper交互的句柄，每次调用open会先关闭上次连接。
     *        句柄保存在_zkHandle中。
     * @return 如果zookeeper_init成功，则返回true，否则返回false。
     */
    bool open();
    /**
     * @brief 把_zkHandle相应的watcher置空，关闭_zkHandle对应的连接(zookeeper_close)
     */
    void close();
    bool isConnected() const;
    bool isConnecting() const;
    bool isBad() const;

    ZkStatus getStatus() const;
    /**
     * @brief 把和zookeeper对应的连接状态进行了分类，分别有ZK_CONNETTED,
     *        ZK_CONNECTING,ZK_BAD
     * @param state:和zookeeper的连接状态.
     * @return 根据state，返回ZK_CONNETTED,ZK_CONNECTING,ZK_BAD中的一种
     */
    static ZkStatus state2Status(int state);

    // NOTICE: ZkWrapper object should not be deleted in callback, which will cause dead lock!!!
    void setConnCallback(const CallbackFuncType& callback);
    void setChildCallback(const CallbackFuncType& callback);
    void setDataCallback(const CallbackFuncType& callback);
    void setCreateCallback(const CallbackFuncType& callback);
    void setDeleteCallback(const CallbackFuncType& callback);
    /**
     * @brief 创建一个zookeeper节点，如果对应的node已经存在，直接设置该节点的内容
     * @param path:节点的路径
     * @param value:节点对应的内容
     * @param permanent:创建的节点的类型是SEQUENCE或EPHEMERAL。默认是EPHEMERAL
     * @return 成功返回true,失败返回false
     */
    virtual bool touch(const std::string& path, const std::string& value, bool permanent = false);
    /**
     * @brief
     */
    bool createPath(const std::string& path);
    /**
     * @brief 得到zookeeper path路径下的节点的孩子节点的名字集合
     * @param path:zookeeper 节点的路径[in]
     * @param vString:path节点下的孩子节点的名字集合[out]
     * @param watch:如果节点改变，server是否要通知client。默认是：不通知
     * @return ZOK成功，其他失败，ZNONODE在有的情况要特殊处理一下
     */
    ZOO_ERRORS getChild(const std::string& path, std::vector<std::string>& vString, bool watch = false);
    /**
     * @brief 得到zookeeper path路径下的节点保存的内容
     * @param str:zookeeper path节点保存的内容[out]
     * @param watch:如果节点改变，server是否要通知client。默认是：不通知
     * @return ZOK成功，其他失败，ZNONODE在有的情况要特殊处理一下
     */
    ZOO_ERRORS getData(const std::string& path, std::string& str, bool watch = false);
    /**
     * @brief 检查zookeeper path节点是否存在。
     * @param path:要检查的zookeeper节点的路径
     * @param bExist:zoo_exists成功处理后，如果存在，则bExist为true，否则为false。
     * @param watch:如果节点改变，server是否要通知client。默认是：不通知
     * @return 成功处理返回true,否则返回false
     */
    bool check(const std::string& path, bool& bExist, bool watch = false);
    /**
     * @brief 删除path路径，包括所有的孩子节点
     * @param path:要删除的路径
     * @return 成功处理返回true,否则返回false
     */
    virtual bool remove(const std::string& path);
    /**
     * @brief 给指定路径写入指定内容
     * @param path:要写入的路径
     * @param value:要写入的内容
     * @return 成功处理返回true,否则返回false
     */
    bool set(const std::string& path, const std::string& value);
    /**
     * @brief 创建一个节点，节点路径会被自动添加递增的序列号，根据参数
     *        permanent的设置也可使节点持久化。
     * @param basePath:要创建的节点路径
     * @param resultPath:节点真正的路径，路径被添加了序列号
     * @param value:节点对应的内容
     * @param permanent: 节点持久化标记
     * @return 成功处理返回true,否则返回false
     */
    bool touchSeq(const std::string& basePath, std::string& resultPath, const std::string& value,
                  bool permanent = false);
    /**
     * @brief 删除zookeerp上的一个节点
     * @param path[in]要删除的节点的路径
     * @return true成功，false失败
     */
    virtual bool deleteNode(const std::string& path);
    // deprecated
    zhandle_t* getZkHandle() const { return _zkHandle; }
    bool createNode(const std::string& path, const std::string& value, bool permanent = false);

private:
    static void watcher(zhandle_t* zk, int type, int state, const char* path, void* context);
    void connectEventCallback(ZkWrapper* zk, const std::string& path, ZkWrapper::ZkStatus status);
    bool connect();
    int getState() const;

    bool createSeqNode(const std::string& path, std::string& resultPath, const std::string& value,
                       bool permanent = false);

    int setNode(const std::string& path, const std::string& str);
    bool createParentPath(const std::string& path);
    friend class ZkDeleter;

private:
    zhandle_t* _zkHandle; // zookeeper交互的句柄, deprecated
    std::string _host;    // zookeeper server的ip和port，格式,ip:port
    unsigned int _timeoutMs; // zookeeper的session超时时间(毫秒)，要求在zk server tickTime的[2倍，20倍]之间

    CallbackFuncType _connCallback;
    CallbackFuncType _childCallback;
    CallbackFuncType _dataCallback;
    CallbackFuncType _createCallback;
    CallbackFuncType _deleteCallback;

    autil::ThreadCond _cond;
    autil::ThreadMutex _mutex;
    autil::ThreadMutex _closingMutex;
    bool _hadConnectOpea; // 是否已经做过连接操作 for ha3

private:
    AUTIL_LOG_DECLARE();
};

} // namespace cm_basic
#endif // ISEARCH_ZKWRAPPER_H
