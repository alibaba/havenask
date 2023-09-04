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
#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"

#include <map>
#include <memory>
#include <unistd.h>

#include "autil/StringUtil.h"

#define SEQIDLENGTH 128

using namespace std;
using namespace std::placeholders;

namespace cm_basic {

AUTIL_LOG_SETUP(cm_basic, ZkWrapper);

#define GET_ZKHANDLE()                                                                                                 \
    shared_ptr<zhandle_t> zkHandle = zkMap.get(this);                                                                  \
    if (!zkHandle) {                                                                                                   \
        AUTIL_LOG(WARN, "invalid zookeeper handle.");                                                                  \
        return false;                                                                                                  \
    }

class ZkDeleter
{
public:
    ZkDeleter(ZkWrapper* zkWrapper_) : zkWrapper(zkWrapper_) {}
    void operator()(zhandle_t* zkHandle)
    {
        if (!zkHandle) {
            return;
        }

        autil::ScopedLock lock(zkWrapper->_closingMutex);
        AUTIL_LOG(INFO, "close zookeeper handle[%p].", zkHandle);
        int ret = zookeeper_close(zkHandle);
        if (ZOK != ret) {
            AUTIL_LOG(WARN, "close zk handle [%p] failed, ret [%d]", zkHandle, ret);
        }
    }

private:
    ZkWrapper* zkWrapper;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(cm_basic, ZkDeleter);

class ZkMap
{
private:
    typedef map<void*, shared_ptr<zhandle_t>> ZKHandleMap;

public:
    ZkMap() {}

    shared_ptr<zhandle_t> get(const ZkWrapper* zkWrapper)
    {
        autil::ScopedLock lock(_mutex);
        auto iter = _zkHandleMap.find((void*)zkWrapper);
        if (iter != _zkHandleMap.end()) {
            return iter->second;
        } else {
            return shared_ptr<zhandle_t>();
        }
    }

    void set(ZkWrapper* zkWrapper, zhandle_t* zkHandle)
    {
        shared_ptr<zhandle_t> zkHandlePtr(zkHandle, ZkDeleter(zkWrapper));
        autil::ScopedLock lock(_mutex);
        _zkHandleMap[zkWrapper] = zkHandlePtr;
    }

    void erase(const ZkWrapper* zkWrapper)
    {
        shared_ptr<zhandle_t> zkHandlePtr;
        {
            autil::ScopedLock lock(_mutex);
            auto it = _zkHandleMap.find((void*)zkWrapper);
            if (it != _zkHandleMap.end()) {
                zkHandlePtr = it->second;
                _zkHandleMap.erase(it);
            }
        }
    }

private:
    autil::ThreadMutex _mutex;
    ZKHandleMap _zkHandleMap;
};

static ZkMap zkMap;

ZkWrapper::ZkWrapper(const std::string& host, unsigned int timeout, bool isMillisecond)
    : _zkHandle(nullptr)
    , _host(host)
    , _hadConnectOpea(false)
{
    if (isMillisecond) {
        _timeoutMs = timeout;
    } else {
        _timeoutMs = timeout * 1000;
    }
    AUTIL_LOG(INFO, "init zk wrapper [%p], host[%s], timeout ms = [%u]", this, _host.c_str(), _timeoutMs);
}

ZkWrapper::~ZkWrapper()
{
    close();
    AUTIL_LOG(INFO, "close zk wrapper [%p], host[%s]", this, _host.c_str());
}

int ZkWrapper::leaderElectorStrategy(std::vector<std::string>& rvec_nodes, const std::string& psz_path,
                                     const std::string& psz_child, bool& b_primary)
{
    std::set<std::string> set_sortnodes;
    std::copy(rvec_nodes.begin(), rvec_nodes.end(), std::inserter(set_sortnodes, set_sortnodes.end()));

    auto iter = set_sortnodes.find(psz_child);
    if (iter == set_sortnodes.end()) {
        AUTIL_LOG(WARN, "LeaderElector can't find self. path(%s), child(%s)", psz_path.c_str(), psz_child.c_str());
        return -1;
    }

    if (iter == set_sortnodes.begin()) {
        b_primary = true;
        return 0;
    } else {
        while (true) {
            --iter;
            bool exist = false;
            if (false == check(psz_path + "/" + *iter, exist, true)) {
                AUTIL_LOG(WARN, "LeaderElector check failed. path(%s), child(%s)", psz_path.c_str(), iter->c_str());
                return -1;
            }
            if (exist) {
                b_primary = false;
                return 0;
            }
            if (iter == set_sortnodes.begin()) {
                b_primary = true;
                return 0;
            }
        }
    }

    return 0;
}

void ZkWrapper::setParameter(const std::string& host, unsigned int timeout_ms)
{
    _host = host;
    _timeoutMs = timeout_ms;
}

void ZkWrapper::setConnCallback(const CallbackFuncType& callback)
{
    autil::ScopedLock lock(_mutex);
    _connCallback = callback;
}

void ZkWrapper::setChildCallback(const CallbackFuncType& callback)
{
    autil::ScopedLock lock(_mutex);
    _childCallback = callback;
}

void ZkWrapper::setDataCallback(const CallbackFuncType& callback)
{
    autil::ScopedLock lock(_mutex);
    _dataCallback = callback;
}

void ZkWrapper::setCreateCallback(const CallbackFuncType& callback)
{
    autil::ScopedLock lock(_mutex);
    _createCallback = callback;
}

void ZkWrapper::setDeleteCallback(const CallbackFuncType& callback)
{
    autil::ScopedLock lock(_mutex);
    _deleteCallback = callback;
}

bool ZkWrapper::touch(const std::string& path, const std::string& value, bool permanent)
{
    GET_ZKHANDLE();
    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", path.c_str());
        return false;
    }

    int ret = setNode(path, value);
    if (ret == ZOK) {
        return true;
    }
    if (ret != ZNONODE) {
        AUTIL_LOG(WARN, "set node failed : path(%s), value size(%lu)", path.c_str(), value.length());
        return false;
    }

    if (createNode(path, value, permanent)) {
        return true;
    } else {
        AUTIL_LOG(WARN, "create node failed : path(%s), value(%s)", path.c_str(), value.c_str());
        return false;
    }
}

bool ZkWrapper::createPath(const std::string& path)
{
    GET_ZKHANDLE();

    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", path.c_str());
        return false;
    }

    return createParentPath(path + "/1");
}

bool ZkWrapper::set(const std::string& path, const std::string& value)
{
    GET_ZKHANDLE();

    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", path.c_str());
        return false;
    }

    if (setNode(path, value) == ZOK) {
        return true;
    }

    AUTIL_LOG(WARN, "set node failed : path(%s), value(%s)", path.c_str(), value.c_str());
    return false;
}

bool ZkWrapper::touchSeq(const std::string& basePath, std::string& resultPath, const std::string& value, bool permanent)
{
    GET_ZKHANDLE();

    if (basePath.size() == 0 || basePath[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", basePath.c_str());
        return false;
    }

    if (createSeqNode(basePath, resultPath, value, permanent)) {
        return true;
    } else {
        AUTIL_LOG(WARN, "create seq node failed : basepath(%s), value(%s)", basePath.c_str(), value.c_str());
        return false;
    }

    return false;
}

ZOO_ERRORS ZkWrapper::getChild(const std::string& path, std::vector<std::string>& vString, bool watch)
{
    shared_ptr<zhandle_t> zkHandle = zkMap.get(this);
    if (!zkHandle) {
        return ZINVALIDSTATE;
    }

    if (path.size() == 0 || path[0] != '/') {
        return ZBADARGUMENTS;
    }

    struct String_vector strings;
    int ret = zoo_get_children(zkHandle.get(), path.c_str(), watch ? 1 : 0, &strings);
    if (ZOK == ret) {
        for (int i = 0; i < strings.count; ++i) {
            vString.push_back(std::string(strings.data[i]));
        }
        deallocate_String_vector(&strings);
    }

    return ZOO_ERRORS(ret);
}

ZOO_ERRORS ZkWrapper::getData(const std::string& path, std::string& str, bool watch)
{
    shared_ptr<zhandle_t> zkHandle = zkMap.get(this);
    if (!zkHandle) {
        return ZINVALIDSTATE;
    }
    if (path.size() == 0 || path[0] != '/') {
        return ZBADARGUMENTS;
    }

    str.clear();
    const int StaticBufferLen = 1024;
    char buffer[StaticBufferLen];
    struct Stat stat;
    int bufferLen = StaticBufferLen;
    unsigned bufferCapacity = StaticBufferLen;
    char* newBuffer = buffer;
    shared_ptr<char[]> newBufferPtr;
    unsigned retryTime = 10;

    for (; retryTime > 0; --retryTime) {
        int ret = zoo_get(zkHandle.get(), path.c_str(), watch ? 1 : 0, newBuffer, &bufferLen, &stat);
        if (ZOK != ret) {
            return ZOO_ERRORS(ret);
        }
        if ((unsigned)stat.dataLength <= bufferCapacity) {
            if (bufferLen > 0) {
                str = std::string(newBuffer, (size_t)bufferLen);
            }
            break;
        }
        newBufferPtr.reset(new char[stat.dataLength]);
        newBuffer = newBufferPtr.get();
        bufferLen = stat.dataLength;
        bufferCapacity = stat.dataLength;
    }
    if (0 == retryTime && bufferCapacity < (unsigned)stat.dataLength) {
        return ZOPERATIONTIMEOUT;
    }
    return ZOK;
}

bool ZkWrapper::check(const std::string& path, bool& bExist, bool watch)
{
    GET_ZKHANDLE();
    bExist = false;
    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", path.c_str());
        return false;
    }

    struct Stat stat;
    int ret = zoo_exists(zkHandle.get(), path.c_str(), watch ? 1 : 0, &stat);
    if (ZOK == ret) {
        bExist = true;
        return true;
    } else if (ZNONODE == ret) {
        bExist = false;
        return true;
    }

    AUTIL_LOG(WARN, "zookeeper check exist failed. %s(%d), path = %s", zerror(ret), ret, path.c_str());
    return false;
}

bool ZkWrapper::remove(const std::string& path)
{
    GET_ZKHANDLE();

    if (path.size() == 0 || path[0] != '/') {
        AUTIL_LOG(WARN, "invalid zookeeper path %s", path.c_str());
        return false;
    }

    int ret = zoo_delete(zkHandle.get(), path.c_str(), -1);
    if ((ZOK == ret) || (ZNONODE == ret)) {
        return true;
    } else if (ZNOTEMPTY == ret) {
        struct String_vector strings;
        ret = zoo_get_children(zkHandle.get(), path.c_str(), 0, &strings);
        if (ZOK == ret) {
            for (int i = 0; i < strings.count; ++i) {
                remove(path + "/" + strings.data[i]);
            }
            deallocate_String_vector(&strings);
        }
        ret = zoo_delete(zkHandle.get(), path.c_str(), -1);
        if ((ZOK == ret) || (ZNONODE == ret)) {
            return true;
        }
    }

    AUTIL_LOG(WARN, "zookeeper delete failed. %s(%d), path = %s", zerror(ret), ret, path.c_str());
    return false;
}

void ZkWrapper::watcher(zhandle_t* zk, int type, int state, const char* path, void* context)
{
    if (!context)
        return;

    ZkWrapper* pthis = (ZkWrapper*)context;
    if (0 != pthis->_closingMutex.trylock()) {
        AUTIL_LOG(INFO, "watcher event ignored by closing! path: %s", path);
        return;
    }

    CallbackFuncType callback;
    bool needCall = true;
    {
        autil::ScopedLock lock(pthis->_mutex);
        if (ZOO_CHANGED_EVENT == type) {
            callback = pthis->_dataCallback;
        } else if (ZOO_CHILD_EVENT == type) {
            callback = pthis->_childCallback;
        } else if (ZOO_SESSION_EVENT == type) {
            pthis->connectEventCallback(pthis, std::string(path), state2Status(state));
            needCall = false;
        } else if (ZOO_CREATED_EVENT == type) {
            callback = pthis->_createCallback;
        } else if (ZOO_DELETED_EVENT == type) {
            callback = pthis->_deleteCallback;
        } else {
            needCall = false;
        }
    }

    if (needCall && callback) {
        callback(pthis, std::string(path), state2Status(state));
    }

    pthis->_closingMutex.unlock();
}

bool ZkWrapper::open()
{
    close();
    return connect();
}

bool ZkWrapper::connect()
{
    _hadConnectOpea = false;
    zhandle_t* zkHandle = zookeeper_init(_host.c_str(), &ZkWrapper::watcher, _timeoutMs, 0, this, 0);
    if (!zkHandle) {
        // don't set debug level to ERROR, heartbeat may print a lot of messages
        AUTIL_LOG(DEBUG, "zookeeper init failed. host: %s", _host.c_str());
        return false;
    }
    zkMap.set(this, zkHandle);
    _zkHandle = zkHandle;

    return true;
}

void ZkWrapper::connectEventCallback(ZkWrapper* zk, const std::string& path, ZkWrapper::ZkStatus status)
{
    if (zk) {
        autil::ScopedLock lock(zk->_cond);
        zk->_cond.broadcast();
        _hadConnectOpea = true;

        if (_connCallback) {
            _connCallback(zk, path, status);
        }
    }
}

void ZkWrapper::close()
{
    _hadConnectOpea = false;
    _zkHandle = nullptr;
    zkMap.erase(this);
}

int ZkWrapper::getState() const
{
    shared_ptr<zhandle_t> zkHandle = zkMap.get(this);
    if (!zkHandle) {
        return ZOO_EXPIRED_SESSION_STATE;
    }
    return zoo_state(zkHandle.get());
}

bool ZkWrapper::isConnected() const { return (ZK_CONNECTED == getStatus()); }

bool ZkWrapper::isConnecting() const { return (ZK_CONNECTING == getStatus()); }

bool ZkWrapper::isBad() const { return (ZK_BAD == getStatus()); }

ZkWrapper::ZkStatus ZkWrapper::getStatus() const { return state2Status(getState()); }

ZkWrapper::ZkStatus ZkWrapper::state2Status(int state)
{
    if (ZOO_CONNECTED_STATE == state) {
        return ZK_CONNECTED;
    } else if ((ZOO_ASSOCIATING_STATE == state) || (ZOO_CONNECTING_STATE == state)) {
        return ZK_CONNECTING;
    }
    return ZK_BAD;
}

bool ZkWrapper::createNode(const std::string& path, const std::string& value, bool permanent)
{
    GET_ZKHANDLE();
    int ret = zoo_create(zkHandle.get(), path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE,
                         permanent ? 0 : ZOO_EPHEMERAL, nullptr, 0);
    if (ZOK == ret) {
        return true;
    } else if (ZNONODE == ret) {
        if (!createParentPath(path)) {
            return false;
        }

        int ret = zoo_create(zkHandle.get(), path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE,
                             permanent ? 0 : ZOO_EPHEMERAL, nullptr, 0);
        if (ZOK == ret) {
            return true;
        } else {
            AUTIL_LOG(WARN, "zookeeper create Node %s failed: %s(%d).", path.c_str(), zerror(ret), ret);
            return false;
        }
    } else {
        AUTIL_LOG(WARN, "zookeeper create Node %s failed: %s(%d).", path.c_str(), zerror(ret), ret);
        return false;
    }
}

bool ZkWrapper::createSeqNode(const std::string& path, std::string& resultPath, const std::string& value,
                              bool permanent)
{
    GET_ZKHANDLE();
    std::vector<char> resultBuff(path.length() + SEQIDLENGTH);
    int ret =
        zoo_create(zkHandle.get(), path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE,
                   permanent ? ZOO_SEQUENCE : (ZOO_SEQUENCE | ZOO_EPHEMERAL), &*resultBuff.begin(), resultBuff.size());
    if (ZOK == ret) {
        resultPath = &*resultBuff.begin();
        return true;
    } else if (ZNONODE == ret) {
        if (!createParentPath(path)) {
            return false;
        }

        int ret = zoo_create(zkHandle.get(), path.c_str(), value.c_str(), value.length(), &ZOO_OPEN_ACL_UNSAFE,
                             permanent ? ZOO_SEQUENCE : (ZOO_SEQUENCE | ZOO_EPHEMERAL), &*resultBuff.begin(),
                             resultBuff.size());
        if (ZOK == ret) {
            resultPath = &*resultBuff.begin();
            return true;
        } else {
            AUTIL_LOG(WARN, "zookeeper create node %s failed: %s(%d).", path.c_str(), zerror(ret), ret);
            return false;
        }
    } else {
        AUTIL_LOG(WARN, "zookeeper create node %s failed: %s(%d).", path.c_str(), zerror(ret), ret);
        return false;
    }
}

bool ZkWrapper::createParentPath(const std::string& path)
{
    GET_ZKHANDLE();
    std::vector<std::string> paths = autil::StringUtil::split(path, "/");
    paths.pop_back();
    std::string current;
    for (auto iter = paths.begin(); iter != paths.end(); ++iter) {
        current += std::string("/") + *iter;
        bool succ = false;
        int mTryCount = autil::StringUtil::split(_host, ",").size();
        for (int tryCount = 0; tryCount < mTryCount; ++tryCount) {
            int ret = zoo_create(zkHandle.get(), current.c_str(), "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, nullptr, 0);
            if (ret == ZOK || ret == ZNODEEXISTS) {
                succ = true;
                break;
            }

            if (ret == ZCONNECTIONLOSS || ret == ZOPERATIONTIMEOUT) {
                AUTIL_LOG(DEBUG, "zookeeper create node %s retry for %s(%d).", current.c_str(), zerror(ret), ret);
                continue;
            }

            AUTIL_LOG(WARN, "zookeeper create node %s failed: %s(%d).", current.c_str(), zerror(ret), ret);
            return false;
        }
        if (!succ) {
            return false;
        }
    }

    return true;
}

bool ZkWrapper::deleteNode(const std::string& path)
{
    GET_ZKHANDLE();
    int ret = zoo_delete(zkHandle.get(), path.c_str(), -1);
    if (ZOK == ret) {
        return true;
    } else {
        return false;
    }
}

int ZkWrapper::setNode(const std::string& path, const std::string& str)
{
    shared_ptr<zhandle_t> zkHandle = zkMap.get(this);
    if (!zkHandle) {
        return ZINVALIDSTATE;
    }
    return zoo_set(zkHandle.get(), path.c_str(), str.c_str(), str.length(), -1);
}

} // namespace cm_basic
