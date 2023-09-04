#ifndef ISEARCH_BS_FILEUTILFORTEST_H
#define ISEARCH_BS_FILEUTILFORTEST_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "fslib/util/FileUtil.h"

namespace build_service { namespace admin {

class FileUtilForTest
{
public:
    FileUtilForTest();
    ~FileUtilForTest();

private:
    FileUtilForTest(const FileUtilForTest&);
    FileUtilForTest& operator=(const FileUtilForTest&);

public:
    // for test, return false check
    static bool checkPathExist(const std::string& path)
    {
        bool exist = false;
        return fslib::util::FileUtil::isExist(path, exist) && exist;
    }
    static bool checkIsFile(const std::string& path)
    {
        bool fileExist = false;
        return fslib::util::FileUtil::isFile(path, fileExist) && fileExist;
    }
    static bool checkIsDir(const std::string& path)
    {
        bool dirExist = false;
        return fslib::util::FileUtil::isDir(path, dirExist) && dirExist;
    }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FileUtilForTest);

}} // namespace build_service::admin

#endif // ISEARCH_BS_FILEUTILFORTEST_H
