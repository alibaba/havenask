#include "fslib/fs/zookeeper/test/ZkServer.h"

#include <errno.h>
#include <sstream>

#include "autil/NetUtil.h"
#include "fslib/fs/FileSystemManager.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, ZkServer);

#define TEST_ZOOKEEPER_PATH "aios/filesystem/fslib/fslib/fs/zookeeper/test/testdata/zookeeper"

ZkServer::ZkServer() {
    _fileSystem = std::make_shared<fslib::fs::zookeeper::ZookeeperFileSystem>();
    FileSystemManager::setFs("zfs", _fileSystem.get());
    start();
}

ZkServer::~ZkServer() {
    stop();
    FileSystemManager::destroyFs("zfs");
}

ZkServer *ZkServer::getZkServer() {
    static ZkServer me;
    if (!me.isStarted()) {
        me.start();
    }
    return &me;
}

unsigned short ZkServer::start() {
    stop();
    _port = autil::NetUtil::randomPort();
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/start.sh " << _port << " " << getpid();
    int ret = system(oss.str().c_str());
    if (ret == 0) {
        AUTIL_LOG(WARN, "Start zookeeper succeed: port=%d", _port);
        usleep(500 * 1000); // wait zk start()
        _isStart = true;
    } else {
        AUTIL_LOG(ERROR, "Start zookeeper failed: port=%d", _port);
    }
    return _port;
}

unsigned short ZkServer::simpleStart() {
    stop();
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/start.sh " << _port << " " << getpid();
    (void)!(system(oss.str().c_str()));
    return _port;
}

void ZkServer::stop() {
    _isStart = false;
    std::ostringstream oss;
    oss << TEST_ZOOKEEPER_PATH << "/stop.sh " << getpid();
    (void)!(system(oss.str().c_str()));
}

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
