#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "fslib/util/FileUtil.h"
#include "worker_framework/WorkerState.h"
#include "worker_framework/ZkState.h"
#include "zookeeper/zookeeper.h"

namespace cm_basic {

ZkWrapper::ZkWrapper(const std::string& host, unsigned int timeout_s, bool isMillisecond) {}

bool ZkWrapper::isBad() const { return false; }

bool ZkWrapper::open() { return true; }

bool ZkWrapper::set(const std::string& path, const std::string& value)
{
    if (!fslib::util::FileUtil::writeFile(path, value)) {
        return false;
    }
    return true;
}

bool ZkWrapper::touch(const std::string& path, const std::string& value, bool permanent)
{
    std::string parentDir = fslib::util::FileUtil::getParentDir(path);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(parentDir, exist)) {
        return false;
    }
    if (!exist && !fslib::util::FileUtil::mkDir(parentDir, true)) {
        return false;
    }
    if (!fslib::util::FileUtil::writeFile(path, value)) {
        return false;
    }
    return true;
}

ZOO_ERRORS ZkWrapper::getData(const std::string& path, std::string& str, bool watch)
{
    if (fslib::util::FileUtil::readFile(path, str)) {
        return ZOK;
    }
    bool exist;
    if (!fslib::util::FileUtil::isExist(path, exist)) {
        return ZCONNECTIONLOSS;
    }
    return !exist ? ZNONODE : ZCONNECTIONLOSS;
}

bool ZkWrapper::check(const std::string& path, bool& bExist, bool watch)
{
    return fslib::util::FileUtil::isExist(path, bExist);
}

ZOO_ERRORS ZkWrapper::getChild(const std::string& path, std::vector<std::string>& vString, bool watch)
{
    if (fslib::util::FileUtil::listDir(path, vString)) {
        return ZOK;
    }
    return ZCONNECTIONLOSS;
}

void ZkWrapper::close() {}

bool ZkWrapper::isConnected() const { return true; }

} // namespace cm_basic

namespace worker_framework {

WorkerState::ErrorCode ZkState::write(const std::string& content) { return WorkerState::EC_OK; }

}; // namespace worker_framework
