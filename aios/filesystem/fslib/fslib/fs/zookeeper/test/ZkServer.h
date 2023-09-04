#pragma once

#include "fslib/common.h"
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class ZkServer {
private:
    ZkServer();
    ~ZkServer();

private:
    ZkServer(const ZkServer &);
    ZkServer &operator=(const ZkServer &);

public:
    static ZkServer *getZkServer();
    unsigned short start();
    void stop();
    unsigned short port() const { return _port; }
    unsigned short simpleStart();
    bool isStarted() const { return _isStart; }

private:
    std::shared_ptr<fslib::fs::zookeeper::ZookeeperFileSystem> _fileSystem;
    unsigned short _port;
    bool _isStart = false;
};

FSLIB_TYPEDEF_SHARED_PTR(ZkServer);

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
