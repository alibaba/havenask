#include "swift/heartbeat/HeartbeatMonitor.h"

#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <unistd.h>
#include <utility>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/StringUtil.h"
#include "fslib/fs/File.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"
#include "swift/heartbeat/ZkHeartbeatWrapper.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace std::placeholders;
using namespace cm_basic;
using namespace swift::protocol;
using namespace fslib::fs;
namespace swift {
namespace heartbeat {

class HeartbeatMonitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void callBack(const protocol::HeartbeatInfo &msg, autil::ThreadCond *);
    void statusChange(cm_basic::ZkWrapper::ZkStatus status, autil::ThreadCond *);
    void checkExtractParameter(const std::string &address,
                               bool f,
                               const std::string &expectHost,
                               const std::string &expectPath);

protected:
    std::string _rand;
    autil::ThreadCond _cond;
    map<string, protocol::HeartbeatInfo> _globalMsg;
    std::string _host;
    int _count;
    bool _statusChange;
    cm_basic::ZkWrapper::ZkStatus _curStatus;
    zookeeper::ZkServer *_zkServer = nullptr;
};

void HeartbeatMonitorTest::setUp() {
    _zkServer = zookeeper::ZkServer::getZkServer();
    _count = 0;
    _statusChange = false;
    _curStatus = ZkWrapper::ZK_BAD;
    unsigned short port = _zkServer->port();
    std::ostringstream oss;
    oss << "127.0.0.1:" << port;
    _host = oss.str();
    _rand = GET_CLASS_NAME();
    cout << "zkPath: " << _host << ", rand: " << _rand << endl;
}

void HeartbeatMonitorTest::tearDown() {}

TEST_F(HeartbeatMonitorTest, testExtractParameter) {
    checkExtractParameter("zfs://127.0.0.1:1234/swift", true, "127.0.0.1:1234", "/swift");

    checkExtractParameter("zfs://127.0.0.1:1234/swift/service/user/", true, "127.0.0.1:1234", "/swift/service/user");

    checkExtractParameter("admin://127.0.0.1:1234/swift/service/user", false, "", "");

    checkExtractParameter("zfs://127.0.0.1:1234/", false, "", "");

    checkExtractParameter("zfs:/", false, "", "");
}

void HeartbeatMonitorTest::checkExtractParameter(const std::string &address,
                                                 bool f,
                                                 const std::string &expectHost,
                                                 const std::string &expectPath) {
    HeartbeatMonitor monitor;
    string host, path;
    ASSERT_EQ(f, monitor.extractParameter(address, host, path));
    if (f) {
        ASSERT_EQ(expectHost, host);
        ASSERT_EQ(expectPath, path);
    }
}

TEST_F(HeartbeatMonitorTest, testStartWithSomeBadServer) {
    string host = "127.0.0.9:9939," + _host;
    string path = "/unittest/HBMT_xxx/" + _rand;

    for (int i = 0; i < 10; ++i) {
        HeartbeatMonitor monitor;

        monitor.setParameter(host, path);
        monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));

        ASSERT_TRUE(monitor.start());
        monitor.stop();
    }
}

TEST_F(HeartbeatMonitorTest, testTrigleCreate) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));

    ASSERT_TRUE(monitor.start());

    HeartbeatInfo msg;
    msg.set_alive(true);
    msg.set_role(ROLE_TYPE_BROKER);

    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    _globalMsg[""].set_role(ROLE_TYPE_ADMIN);
    autil::ScopedLock lock(_cond);
    ASSERT_TRUE(zk.touch(path + "/a", msg));
    _cond.wait();
    ASSERT_EQ(_globalMsg[""].role(), ROLE_TYPE_BROKER);
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testTrigleChange) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    HeartbeatInfo msg;
    msg.set_role(ROLE_TYPE_BROKER);
    {
        autil::ScopedLock lock(_cond);
        ASSERT_TRUE(zk.touch(path + "/a", msg));
        _cond.wait();
        ASSERT_EQ(_globalMsg[""].role(), ROLE_TYPE_BROKER);
    }

    msg.set_role(ROLE_TYPE_ADMIN);

    _globalMsg[""].set_role(ROLE_TYPE_NONE);
    {
        autil::ScopedLock lock(_cond);
        ASSERT_TRUE(zk.touch(path + "/a", msg));
        _cond.wait();
        ASSERT_EQ(_globalMsg[""].role(), ROLE_TYPE_ADMIN);
    }
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testTrigleDelete) {
    HeartbeatInfo msg;
    msg.set_alive(true);

    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    {
        autil::ScopedLock lock(_cond);
        {
            ZkHeartbeatWrapper zk;
            HeartbeatInfo value;
            zk.setParameter(host);
            ASSERT_TRUE(zk.open());
            value.set_alive(true);
            _globalMsg[""].set_alive(false);
            ASSERT_TRUE(zk.touch(path + "/a", value));
            _cond.wait();
            ASSERT_TRUE(_globalMsg[""].alive());
        }
        _cond.wait();
        ASSERT_TRUE(!_globalMsg[""].alive());
    }

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testStartWithoutSetParameter) {
    HeartbeatMonitor monitor;
    ASSERT_TRUE(!monitor.start());
}

TEST_F(HeartbeatMonitorTest, testTrigleAtSetHandler) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    ASSERT_TRUE(monitor.start());
    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;

    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    msg.set_role(ROLE_TYPE_ADMIN);
    ASSERT_TRUE(zk.touch(path + "/a", msg));

    while (1) {
        usleep(10);
        autil::ScopedLock lock(monitor._mutex);
        if (monitor._localWorkerImage.size() >= 1) {
            break;
        }
    }

    _globalMsg[""].set_role(ROLE_TYPE_NONE);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
    ASSERT_TRUE(_globalMsg[""].role() != ROLE_TYPE_ADMIN);

    msg.set_role(ROLE_TYPE_BROKER);
    autil::ScopedLock lock(_cond);
    ASSERT_TRUE(zk.touch(path + "/b", msg));
    _cond.wait();
    ASSERT_EQ(size_t(2), monitor._localWorkerImage.size());
    ASSERT_EQ(_globalMsg[""].role(), ROLE_TYPE_BROKER);

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldCreateDeleteCreateRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_alive(true);
    _count = 0;
    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_ADMIN);
        _cond.lock();
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        while (1) {
            usleep(10);
            if (monitor._localWorkerImage.size() >= 1) {
                if (monitor._localWorkerImage.begin()->second.alive()) {
                    break;
                }
            }
        }
    }

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_BROKER);
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        _cond.wait();
        if (2 != _count) {
            ASSERT_EQ(1, _count);
            _cond.wait();
        }
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
        ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_BROKER);
        _cond.unlock();
    }

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldDeleteCreateRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_BROKER);

        _cond.lock();

        ASSERT_TRUE(zk.touch(path + "/a", msg));

        _cond.wait();
    }

    _count = 0;
    while (1) {
        usleep(10);
        if (monitor._localWorkerImage.size() == 0) {
            break;
        }
    }

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_ADMIN);
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        _cond.wait();
        if (2 != _count) {
            ASSERT_EQ(1, _count);
            ASSERT_TRUE(!_globalMsg[""].alive());
            _cond.wait();
        }
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
        ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_ADMIN);
        _cond.unlock();
    }

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldDeleteModifyRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_role(ROLE_TYPE_BROKER);
    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    ASSERT_TRUE(zk.touch(path + "/b", msg));
    while (1) {
        usleep(10);
        if (monitor._localWorkerImage.size() == 1) {
            break;
        }
    }

    {
        ZkHeartbeatWrapper zk2;
        zk2.setParameter(host);
        ASSERT_TRUE(zk2.open());
        msg.set_role(ROLE_TYPE_ADMIN);

        _cond.lock();
        ASSERT_TRUE(zk2.touch(path + "/a", msg));
        // lock delete
        _cond.wait();
    }

    _count = 0;
    while (1) {
        usleep(10);
        if (monitor._localWorkerImage.size() == 1) {
            break;
        }
    }

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        // modify node
        msg.set_role(ROLE_TYPE_ADMIN);

        ASSERT_TRUE(zk.touch(path + "/b", msg));

        _cond.wait();
        if (2 != _count) {
            ASSERT_EQ(1, _count);
            ASSERT_TRUE(!_globalMsg[""].alive());
            _cond.wait();
        }
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
        ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_ADMIN);
        _cond.unlock();
    }

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldDeleteDeleteRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_role(ROLE_TYPE_BROKER);
    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());
        _cond.lock();
        ASSERT_TRUE(zk.touch(path + "/b", msg));
        _cond.wait();
        {
            ZkHeartbeatWrapper zk2;
            zk2.setParameter(host);
            ASSERT_TRUE(zk2.open());

            ASSERT_TRUE(zk2.touch(path + "/a", msg));
            // lock delete
            _cond.wait();
            ASSERT_EQ(size_t(2), monitor._localWorkerImage.size());
        }

        _count = 0;
        while (1) {
            usleep(10);
            if (monitor._localWorkerImage.size() == size_t(1)) {
                break;
            }
        }
    }

    _count = 0;
    _cond.wait();
    if (2 != _count) {
        ASSERT_EQ(1, _count);
        ASSERT_TRUE(!_globalMsg[""].alive());
        _cond.wait();
    }
    ASSERT_EQ(size_t(0), monitor._localWorkerImage.size());
    ASSERT_EQ(2, _count);
    _cond.unlock();
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldCreateCreateRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_alive(true);
    _count = 0;
    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    msg.set_role(ROLE_TYPE_BROKER);
    _cond.lock();
    ASSERT_TRUE(zk.touch(path + "/a", msg));

    while (1) {
        usleep(10);
        if (monitor._localWorkerImage.size() >= 1) {
            if (monitor._localWorkerImage.begin()->second.alive()) {
                break;
            }
        }
    }

    ZkHeartbeatWrapper zk2;
    zk2.setParameter(host);
    ASSERT_TRUE(zk2.open());

    msg.set_role(ROLE_TYPE_ADMIN);
    ASSERT_TRUE(zk2.touch(path + "/b", msg));

    _cond.wait();
    if (2 != _count) {
        ASSERT_EQ(1, _count);
        _cond.wait();
    }
    ASSERT_EQ(size_t(2), monitor._localWorkerImage.size());
    ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_ADMIN);
    _cond.unlock();

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldModifyModifyRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;

    _count = 0;

    ZkHeartbeatWrapper zk;
    zk.setParameter(host);
    ASSERT_TRUE(zk.open());

    msg.set_role(ROLE_TYPE_BROKER);
    // create
    _cond.lock();
    ASSERT_TRUE(zk.touch(path + "/a", msg));
    _cond.wait();

    msg.set_role(ROLE_TYPE_ADMIN);
    // modify
    ASSERT_TRUE(zk.touch(path + "/a", msg));

    while (1) {
        usleep(10);
        if (monitor._localWorkerImage.size() == 1) {
            if (monitor._localWorkerImage.begin()->second.role() == ROLE_TYPE_ADMIN) {
                break;
            }
        }
    }

    msg.set_role(ROLE_TYPE_NONE);
    // modify
    ASSERT_TRUE(zk.touch(path + "/a", msg));

    _count = 0;
    _cond.wait();
    if (2 != _count) {
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
        ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_ADMIN);
        ASSERT_EQ(1, _count);
        _cond.wait();
    }
    ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
    ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_NONE);
    _cond.unlock();
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldModifyDeleteRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;

    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_BROKER);
        msg.set_alive(true);
        _cond.lock();
        ASSERT_TRUE(zk.touch(path + "/a", msg));
        _cond.wait();
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());

        // modify node
        msg.set_role(ROLE_TYPE_ADMIN);
        ASSERT_TRUE(zk.touch(path + "/a", msg));
        while (1) {
            usleep(10);
            if (monitor._localWorkerImage.size() == 1) {
                if (monitor._localWorkerImage.begin()->second.role() == ROLE_TYPE_ADMIN) {
                    break;
                }
            }
        }
    }
    _count = 0;
    _cond.wait();
    if (2 != _count) {
        ASSERT_EQ(1, _count);
        ASSERT_TRUE(_globalMsg[""].alive());
        _cond.wait();
    }
    ASSERT_EQ(size_t(0), monitor._localWorkerImage.size());
    ASSERT_TRUE(!_globalMsg[""].alive());
    _cond.unlock();
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldCreateModifyRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_alive(true);
    _count = 0;
    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_ADMIN);
        _cond.lock();
        // create
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        while (1) {
            usleep(10);
            if (monitor._localWorkerImage.size() >= 1) {
                if (monitor._localWorkerImage.begin()->second.alive()) {
                    break;
                }
            }
        }

        msg.set_role(ROLE_TYPE_BROKER);
        // modify
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        _cond.wait();
        if (2 != _count) {
            ASSERT_EQ(1, _count);
            _cond.wait();
        }
        ASSERT_EQ(size_t(1), monitor._localWorkerImage.size());
        ASSERT_TRUE(_globalMsg[""].role() == ROLE_TYPE_BROKER);
        _cond.unlock();
    }

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testHoldCreateDeleteRelease) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());

    string workDir = "/testTrigleAtSetHandler/";
    HeartbeatInfo msg;
    msg.set_alive(true);
    _count = 0;
    {
        ZkHeartbeatWrapper zk;
        zk.setParameter(host);
        ASSERT_TRUE(zk.open());

        msg.set_role(ROLE_TYPE_BROKER);
        _cond.lock();
        ASSERT_TRUE(zk.touch(path + "/a", msg));

        while (1) {
            usleep(10);
            if (monitor._localWorkerImage.size() >= 1) {
                if (monitor._localWorkerImage.begin()->second.alive()) {
                    break;
                }
            }
        }
    }

    _cond.wait();
    if (2 != _count) {
        ASSERT_EQ(1, _count);
        _cond.wait();
    }
    ASSERT_EQ(size_t(0), monitor._localWorkerImage.size());
    ASSERT_TRUE(!_globalMsg[""].alive());
    _cond.unlock();

    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testTrigleAtStart) {
    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/HBMT_1/" + _rand;

    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));

    HeartbeatInfo msg;
    string workDir = "/testTrigleAtStart";
    msg.set_role(ROLE_TYPE_BROKER);

    ZkHeartbeatWrapper zk[8];
    for (int i = 0; i < 8; ++i) {
        zk[i].setParameter(host);
        ASSERT_TRUE(zk[i].open());
        ASSERT_TRUE(zk[i].touch(path + "/a" + autil::StringUtil::toString(i), msg));
    }

    _globalMsg[""].set_role(ROLE_TYPE_NONE);
    _count = 0;
    ASSERT_TRUE(monitor.start());
    ASSERT_EQ(_globalMsg[""].role(), ROLE_TYPE_BROKER);
    ASSERT_EQ(8, _count);
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testDisconnected) {
    HeartbeatInfo msg;
    string workDir;
    msg.set_role(ROLE_TYPE_BROKER);
    msg.set_alive(true);
    msg.set_address("a");

    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/testDisconnected/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());
    _count = 0;
    ZkHeartbeatWrapper zk;
    {
        autil::ScopedLock lock(_cond);
        {
            HeartbeatInfo value;
            value.set_role(ROLE_TYPE_ADMIN);
            value.set_alive(true);
            value.set_address("a");
            zk.setParameter(host);
            ASSERT_TRUE(zk.open());

            _globalMsg["a"].set_alive(false);
            ASSERT_TRUE(zk.touch(path + "/a", value));
            _cond.wait();
            ASSERT_TRUE(_globalMsg["a"].alive());
            ASSERT_EQ(ROLE_TYPE_ADMIN, _globalMsg["a"].role());
        }
    }
    monitor._zk.close();
    {
        {
            HeartbeatInfo value;
            value.set_role(ROLE_TYPE_BROKER);
            value.set_alive(true);
            value.set_address("b");
            zk.setParameter(host);
            ASSERT_TRUE(zk.open());
            _globalMsg["b"].set_alive(false);
            ASSERT_TRUE(zk.touch(path + "/b", value));
            monitor._zk.open();
            autil::ScopedLock lock(_cond);
            while (_count != 3) {
                _cond.wait();
            }

            ASSERT_TRUE(!_globalMsg["a"].alive());
            ASSERT_EQ(ROLE_TYPE_ADMIN, _globalMsg["a"].role());

            ASSERT_TRUE(_globalMsg["b"].alive());
            ASSERT_EQ(ROLE_TYPE_BROKER, _globalMsg["b"].role());
        }
    }
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testDisconnectedNotify) {
    HeartbeatInfo msg;
    string workDir;
    msg.set_role(ROLE_TYPE_BROKER);
    msg.set_alive(true);

    HeartbeatMonitor monitor;
    ASSERT_TRUE(true == monitor.isBad());
    string host = _host;
    string path = "/unittest/testDisconnectedNotify/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(!monitor._isConnected);
    ASSERT_TRUE(monitor.start());
    ASSERT_TRUE(monitor._isConnected);
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testDisconnectedNotifyWithConnectedFail) {
    HeartbeatInfo msg;
    msg.set_alive(true);

    HeartbeatMonitor monitor;
    ASSERT_TRUE(true == monitor.isBad());
    string host = "222.0.0.1:5687";
    string path = "/unittest/testDisconnectedNotify/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(!monitor._isConnected);
    ASSERT_TRUE(!monitor.start());
    ASSERT_TRUE(!monitor._isConnected);
    ASSERT_TRUE(!monitor.start());
    ASSERT_TRUE(!monitor._isConnected);
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testIsBad) {
    HeartbeatMonitor monitor;
    ASSERT_TRUE(true == monitor.isBad());
    string host = "251.0.1.1:2345";
    string path = "/unittest/testIsBad/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.isBad());
    ASSERT_TRUE(!monitor.start());
    ASSERT_TRUE(monitor.isBad());
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testDisconnectedAndDeadNode) {
    HeartbeatInfo msg;
    msg.set_role(ROLE_TYPE_BROKER);
    msg.set_alive(true);

    HeartbeatMonitor monitor;
    string host = _host;
    string path = "/unittest/testDisconnected/" + _rand;
    monitor.setParameter(host, path);
    monitor.setHandler(bind(&HeartbeatMonitorTest::callBack, this, _1, &_cond));
    ASSERT_TRUE(monitor.start());
    _count = 0;

    {
        ZkHeartbeatWrapper zk;
        autil::ScopedLock lock(_cond);
        {
            HeartbeatInfo value;
            value.set_alive(true);
            value.set_role(ROLE_TYPE_ADMIN);
            zk.setParameter(host);
            ASSERT_TRUE(zk.open());

            _globalMsg[""].set_alive(false);
            ASSERT_TRUE(zk.touch(path + "/a", value));
            _cond.wait();
            ASSERT_TRUE(_globalMsg[""].alive());
            ASSERT_EQ(ROLE_TYPE_ADMIN, _globalMsg[""].role());
        }
        monitor._zk.close();
    }
    ASSERT_TRUE(monitor.isBad());
    monitor._zk.open();
    {
        autil::ScopedLock lock(_cond);
        while (_count != 2) {
            _cond.wait();
        }
        ASSERT_TRUE(!_globalMsg[""].alive());
    }
    monitor.stop();
}

TEST_F(HeartbeatMonitorTest, testStatusChangeNotify) {
    HeartbeatMonitor monitor;
    ASSERT_TRUE(true == monitor.isBad());
    string host = _host;
    string path = "/unittest/testStatusChangeNotify/" + _rand;
    monitor.setParameter(host, path);
    monitor.setStatusChangeHandler(bind(&HeartbeatMonitorTest::statusChange, this, _1, &_cond));

    _statusChange = false;
    ASSERT_TRUE(monitor.start());
    {
        autil::ScopedLock lock(_cond);
        while (!_statusChange) {
            _cond.wait();
        }
    }

    ASSERT_EQ(_curStatus, ZkWrapper::ZK_CONNECTED);

    _statusChange = false;
    monitor.stop();
    {
        autil::ScopedLock lock(_cond);
        while (!_statusChange) {
            _cond.wait();
        }
    }

    ASSERT_EQ(_curStatus, ZkWrapper::ZK_BAD);
}

TEST_F(HeartbeatMonitorTest, testDisconnectedStatusNotify) {
    HeartbeatMonitor monitor;
    ASSERT_TRUE(true == monitor.isBad());
    string host = _host;
    string path = "/unittest/testDisconnectedStatusNotify/" + _rand;
    monitor.setParameter(host, path);
    monitor.setStatusChangeHandler(bind(&HeartbeatMonitorTest::statusChange, this, _1, &_cond));

    _statusChange = false;
    ASSERT_TRUE(monitor.start());
    {
        autil::ScopedLock lock(_cond);
        while (!_statusChange) {
            _cond.wait();
        }
    }

    ASSERT_EQ(_curStatus, ZkWrapper::ZK_CONNECTED);

    _statusChange = false;
    _zkServer->stop();
    {
        autil::ScopedLock lock(_cond);
        while (!_statusChange) {
            _cond.wait();
        }
    }

    ASSERT_EQ(_curStatus, ZkWrapper::ZK_BAD);
}

TEST_F(HeartbeatMonitorTest, testCheckAndInsert) {
    HeartbeatMonitor monitor;
    string path;
    HeartbeatInfo msg;

    msg.set_heartbeatid(90);
    EXPECT_EQ(0, monitor._localWorkerImage.size());
    ASSERT_TRUE(monitor.checkAndInsert("path_1", msg));
    EXPECT_EQ(1, monitor._localWorkerImage.size());
    auto it = monitor._localWorkerImage.find("path_1");
    EXPECT_TRUE(monitor._localWorkerImage.end() != it);
    EXPECT_EQ(90, (it->second).heartbeatid());

    msg.set_heartbeatid(80);
    ASSERT_FALSE(monitor.checkAndInsert("path_1", msg));
    EXPECT_EQ(1, monitor._localWorkerImage.size());
    it = monitor._localWorkerImage.find("path_1");
    EXPECT_TRUE(monitor._localWorkerImage.end() != it);
    EXPECT_EQ(90, (it->second).heartbeatid());

    msg.set_heartbeatid(100);
    ASSERT_FALSE(monitor.checkAndInsert("path_1", msg));
    EXPECT_EQ(1, monitor._localWorkerImage.size());
    it = monitor._localWorkerImage.find("path_1");
    EXPECT_TRUE(monitor._localWorkerImage.end() != it);
    EXPECT_EQ(90, (it->second).heartbeatid()); // same content except heartbeatid

    msg.set_heartbeatid(105);
    msg.set_sessionid(1234);
    ASSERT_TRUE(monitor.checkAndInsert("path_1", msg));
    EXPECT_EQ(1, monitor._localWorkerImage.size());
    it = monitor._localWorkerImage.find("path_1");
    EXPECT_TRUE(monitor._localWorkerImage.end() != it);
    EXPECT_EQ(105, (it->second).heartbeatid());
    EXPECT_EQ(1234, (it->second).sessionid());

    msg.set_heartbeatid(110);
    ASSERT_TRUE(monitor.checkAndInsert("path_2", msg));
    EXPECT_EQ(2, monitor._localWorkerImage.size());
    it = monitor._localWorkerImage.find("path_2");
    EXPECT_TRUE(monitor._localWorkerImage.end() != it);
    EXPECT_EQ(110, (it->second).heartbeatid());
}

void HeartbeatMonitorTest::statusChange(ZkWrapper::ZkStatus status, autil::ThreadCond *cond) {
    autil::ScopedLock lock(*cond);
    _statusChange = true;
    _curStatus = status;
    cond->signal();
}

void HeartbeatMonitorTest::callBack(const HeartbeatInfo &msg, autil::ThreadCond *cond) {
    autil::ScopedLock lock(*cond);
    _globalMsg[msg.address()] = msg;
    ++_count;
    cond->signal();
}

} // namespace heartbeat
} // namespace swift
