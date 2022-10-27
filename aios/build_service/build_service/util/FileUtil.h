#ifndef ISEARCH_BS_FILEUTIL_H
#define ISEARCH_BS_FILEUTIL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <string>
#include <vector>

namespace build_service {
namespace util {

class FileUtil
{
public:
    static bool readFile(const std::string &file, std::string &content);
    static bool writeFile(const std::string &file, const std::string &content);

    static bool mkDir(const std::string &dirPath, bool recursive = false);
    static bool remove(const std::string &path);
    static bool removeIfExist(const std::string &path);

    static bool listDir(const std::string &path,
                        std::vector<std::string> &fileList);
    static bool listDirRecursive(const std::string &path,
                                 std::vector<std::string>& entryVec,
                                 bool fileOnly);
    static bool listDirWithAbsolutePath(const std::string &path,
            std::vector<std::string> &entryVec, bool fileOnly);
    static bool listDirRecursiveWithAbsolutePath(const std::string &path,
            std::vector<std::string> &entryVec, bool fileOnly);

    static bool isDir(const std::string &path, bool &dirExist);
    static bool isFile(const std::string &path, bool &fileExist);
    static bool isExist(const std::string &path, bool &exist);

    static std::string joinFilePath(const std::string &dir, const std::string &file);
    static std::string normalizeDir(const std::string &dirName);
    static void splitFileName(const std::string &input,
                              std::string &folder, std::string &fileName);
    static std::string getParentDir(const std::string &dir);

    // zookeeper
    static std::string getHostFromZkPath(const std::string &zkPath);
    static std::string getPathFromZkPath(const std::string &zkPath);

    static bool getFileLength(const std::string &filePath, int64_t &fileLength,
                              bool needPrintLog = true);

    static bool readWithBak(const std::string filepath, std::string &content);
    static bool writeWithBak(const std::string filepath, const std::string &content);
    static bool atomicCopy(const std::string &src, const std::string &file);
private:
    static bool listDir(const std::string &path,
                        std::vector<std::string>& entryVec,
                        bool fileOnly,
                        bool recursive);
private:
    static const std::string ZFS_PREFIX;
    static const size_t ZFS_PREFIX_LEN;
    static const std::string TMP_SUFFIX;
    static const std::string BAK_SUFFIX;
private:
    FileUtil();
    ~FileUtil();
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_FILEUTIL_H
