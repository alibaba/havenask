#ifndef FSLIB_PLUGIN_FSCASEHELPERBASE_H
#define FSLIB_PLUGIN_FSCASEHELPERBASE_H

#include "autil/Log.h"
#include "fslib/common/common_define.h"
#include "fslib/common/common_type.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(zookeeper);

class FsCaseHelperBase {
public:
    virtual ~FsCaseHelperBase() {}

public:
    virtual std::string getFullName(std::string pathName) = 0;
    virtual std::string getDstPath(std::string fullName) = 0;
    virtual std::string getTestDir() = 0;
    virtual std::string getDifferentFsFileName() = 0;
    virtual std::string getNotExistFileName() = 0;

    virtual bool exist(const std::string &pathName) = 0;

    virtual int read(const std::string &fileName, void *buffer, int len) = 0;
    virtual int write(const std::string &fileName, const void *buffer, int len) = 0;
    virtual int append(const std::string &fileName, const void *buffer, int len) = 0;

    virtual bool changeMode(const std::string &fileName, const std::string &mode) = 0;

    virtual void mkdir(const std::string &dirname) = 0;
    virtual void createFile(const std::string &filename) = 0;
    virtual void remove(const std::string &path) = 0;
    virtual std::string changeDir(const std::string &dstDir) = 0;
};

typedef std::unique_ptr<FsCaseHelperBase> FsCaseHelperBasePtr;

FSLIB_PLUGIN_END_NAMESPACE(zookeeper);

#endif // FSLIB_PLUGIN_FSCASEHELPERBASE_H
