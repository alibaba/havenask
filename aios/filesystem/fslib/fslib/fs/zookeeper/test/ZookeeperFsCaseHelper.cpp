#include "fslib/fs/zookeeper/test/ZookeeperFsCaseHelper.h"

#include "autil/StringUtil.h"
#include "fslib/util/SafeBuffer.h"

using namespace std;
using namespace fslib;
using namespace fslib::util;

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);
AUTIL_DECLARE_AND_SETUP_LOGGER(zookeeper, ZookeeperFsCaseHelper);

ZookeeperFsCaseHelper::ZookeeperFsCaseHelper() {
    _zkServer = ZkServer::getZkServer();
    _zkSpec = std::string("127.0.0.1:") + autil::StringUtil::toString(_zkServer->port());
}

ZookeeperFsCaseHelper::~ZookeeperFsCaseHelper() {}

FsType getFsType(const std::string &path) {
    if (path.size() == 0) {
        return FSLIB_FS_UNKNOWN_FILESYSTEM;
    }

    size_t found = path.find("://");
    if (found == string::npos) {
        return FSLIB_FS_UNKNOWN_FILESYSTEM;
    } else {
        return path.substr(0, found);
    }
}

bool internalParse(const string &src, string &dst, FsType &type) {
    type = getFsType(src);
    if (type != "zfs") {
        AUTIL_LOG(ERROR, "unknown file type for path %s", src.c_str());
        return false;
    }
    AUTIL_LOG(DEBUG, "file type for path %s is %s.", src.c_str(), type.c_str());

    dst.reserve(src.size());
    dst.resize(0);

    size_t prefixLen = ZookeeperFileSystem::ZOOKEEPER_PREFIX.size();
    size_t beginPosForFileName = src.find('/', prefixLen);
    if (beginPosForFileName == string::npos) {
        AUTIL_LOG(ERROR,
                  "parse zookeeper config fail for path %s which "
                  "is an invalid path",
                  src.c_str());
        return false;
    }

    string configStr;
    configStr.reserve(beginPosForFileName);
    configStr.append(src.c_str(), beginPosForFileName);
    size_t beginPosForConfig = configStr.find(ZookeeperFileSystem::CONFIG_SEPARATOR.c_str(), prefixLen);
    configStr.resize(0);
    if (beginPosForConfig != string::npos) {
        configStr.append(src.c_str() + beginPosForConfig + 1, beginPosForFileName - beginPosForConfig - 1);
        dst.append(src.c_str() + prefixLen, beginPosForConfig - prefixLen);
    } else {
        dst.append(src.c_str() + prefixLen, beginPosForFileName - prefixLen);
    }

    if (!ZookeeperFileSystem::eliminateSeqSlash(
            src.c_str() + beginPosForFileName, src.size() - beginPosForFileName, dst, false)) {
        AUTIL_LOG(ERROR,
                  "parse zookeeper config fail for path %s which "
                  "is an invalid path",
                  src.c_str());
        return false;
    }
    return true;
}

string ZookeeperFsCaseHelper::getFullName(string dirName) {
    return ZookeeperFileSystem::ZOOKEEPER_PREFIX + _zkSpec + "?retry=10" + dirName;
}

string ZookeeperFsCaseHelper::getDstPath(string fullName) {
    string dstPath;
    FsType type;
    internalParse(fullName, dstPath, type);
    return dstPath;
}

string ZookeeperFsCaseHelper::getTestDir() { return ""; }

string ZookeeperFsCaseHelper::getDifferentFsFileName() { return "hdfs://127.0.0.1:10/home/text"; }

string ZookeeperFsCaseHelper::getNotExistFileName() {
    return ZookeeperFileSystem::ZOOKEEPER_PREFIX + _zkSpec + "?retry=10" + "/notexistingfile";
}

bool ZookeeperFsCaseHelper::exist(const string &pathName) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(pathName);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        if (path.size() > 1 && path[path.size() - 1] == '/') {
            path.resize(path.size() - 1);
        }

        int zrc = zoo_exists(zh, path.c_str(), 0, NULL);
        zookeeper_close(zh);

        return zrc == ZOK;
    } else {
        return false;
    }
}

int ZookeeperFsCaseHelper::read(const string &fileName, void *buffer, int len) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(fileName);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        zoo_get(zh, path.c_str(), 0, (char *)buffer, &len, NULL);
        zookeeper_close(zh);

        return (len > 0) ? len : 0;
    } else {
        return -1;
    }
}

int ZookeeperFsCaseHelper::write(const string &fileName, const void *buffer, int len) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(fileName);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);
    if (zh) {
        if (zoo_exists(zh, path.c_str(), 0, NULL) != ZOK) {
            internalCreateNode(zh, path);
        }
        zoo_set(zh, path.c_str(), (char *)buffer, len, -1);
        zookeeper_close(zh);

        return len;
    } else {
        return -1;
    }
}

int ZookeeperFsCaseHelper::append(const string &fileName, const void *buffer, int len) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(fileName);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        struct Stat stat;
        if (zoo_exists(zh, path.c_str(), 0, &stat) != ZOK) {
            internalCreateNode(zh, path);

            zoo_set(zh, path.c_str(), (char *)buffer, len, -1);
        } else {
            int oldLen = stat.dataLength;

            SafeBuffer safeBuf(oldLen + len + 1);
            zoo_get(zh, path.c_str(), 0, (char *)safeBuf.getBuffer(), &oldLen, NULL);
            memcpy(safeBuf.getBuffer() + oldLen, buffer, len);
            safeBuf.getBuffer()[oldLen + len] = '\0';

            zoo_set(zh, path.c_str(), (char *)safeBuf.getBuffer(), oldLen + len, -1);
        }
        zookeeper_close(zh);

        return len;
    } else {
        return -1;
    }
}

bool ZookeeperFsCaseHelper::changeMode(const string &fileName, const string &mode) { return false; }

void ZookeeperFsCaseHelper::mkdir(const string &dirname) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(dirname);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        internalCreateNode(zh, path);
        zookeeper_close(zh);
    }
}

void ZookeeperFsCaseHelper::createFile(const string &filename) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(filename);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        internalCreateNode(zh, path);
        zookeeper_close(zh);
    }
}

void ZookeeperFsCaseHelper::remove(const string &pathName) {
    zhandle_t *zh = NULL;
    string dstPath = getDstPath(pathName);
    string path;
    internalGetZhandleAndPath(dstPath, zh, path);

    if (zh) {
        internalRemoveNode(zh, path);
        zookeeper_close(zh);
    }
}

void ZookeeperFsCaseHelper::internalGetZhandleAndPath(const string &pathName, zhandle_t *&zh, string &path) {
    string server;
    size_t pos = pathName.find("/");
    if (pos != string::npos) {
        server = pathName.substr(0, pos);
        path = pathName.substr(pos);
        zh = zookeeper_init(server.c_str(), NULL, 1800, 0, NULL, 0);
    } else {
        zh = NULL;
    }
}

void ZookeeperFsCaseHelper::internalCreateNode(zhandle_t *zh, const string &node) {
    size_t pos = node.rfind("/");
    int zrc = ZOK;
    string parentNode;
    if (pos != 0) {
        parentNode = node.substr(0, pos);
        zrc = zoo_exists(zh, parentNode.c_str(), 0, NULL);
    }

    if (zrc == ZOK) {
        zrc = zoo_create(zh, node.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    } else {
        internalCreateNode(zh, parentNode);
        zrc = zoo_create(zh, node.c_str(), NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
    }
}

void ZookeeperFsCaseHelper::internalRemoveNode(zhandle_t *zh, const string &node) {
    struct String_vector strings;

    int ret = zoo_get_children(zh, node.c_str(), 0, &strings);
    if (ret == ZOK) {
        for (int i = 0; i < strings.count; i++) {
            string childName = node + "/" + string(strings.data[i]);
            internalRemoveNode(zh, childName);
        }
        freeStringVector(&strings);
    }

    zoo_delete(zh, node.c_str(), -1);
}

void ZookeeperFsCaseHelper::freeStringVector(struct String_vector *v) {
    if (v->data) {
        for (int32_t i = 0; i < v->count; i++) {
            free(v->data[i]);
        }
        free(v->data);
        v->data = NULL;
    }
}

string ZookeeperFsCaseHelper::changeDir(const string &dstDir) { return ""; }

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);
