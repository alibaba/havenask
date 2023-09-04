#ifndef FSLIB_PLUGIN_ZOOKEEPERFSCASEHELPER_H
#define FSLIB_PLUGIN_ZOOKEEPERFSCASEHELPER_H

#include "fslib/common.h"
#include "fslib/fs/zookeeper/ZookeeperFileSystem.h"
#include "fslib/fs/zookeeper/test/FsCaseHelperBase.h"
#include "fslib/fs/zookeeper/test/ZkServer.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class ZookeeperFsCaseHelper : public FsCaseHelperBase {
public:
    ZookeeperFsCaseHelper();
    ~ZookeeperFsCaseHelper();

public:
    /*override*/ std::string getFullName(std::string dirName);
    /*override*/ std::string getDstPath(std::string fullName);
    /*override*/ std::string getTestDir();
    /*override*/ std::string getDifferentFsFileName();
    /*override*/ std::string getNotExistFileName();

    /*override*/ bool exist(const std::string &pathName);

    /*override*/ int read(const std::string &fileName, void *buffer, int len);
    /*override*/ int write(const std::string &fileName, const void *buffer, int len);
    /*override*/ int append(const std::string &fileName, const void *buffer, int len);

    /*override*/ bool changeMode(const std::string &fileName, const std::string &mode);

    /*override*/ void mkdir(const std::string &dirname);
    /*override*/ void createFile(const std::string &filename);
    /*override*/ void remove(const std::string &path);
    /*override*/ std::string changeDir(const std::string &dstDir);

private:
    static void freeStringVector(struct String_vector *v);
    void internalGetZhandleAndPath(const std::string &pathName, zhandle_t *&zh, std::string &path);

    void internalCreateNode(zhandle_t *zh, const std::string &node);

    void internalRemoveNode(zhandle_t *zh, const std::string &node);

private:
    ZkServer *_zkServer;
    std::string _zkSpec;
};

typedef std::unique_ptr<ZookeeperFsCaseHelper> ZookeeperFsCaseHelperPtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_ZOOKEEPERFSCASEHELPER_H
