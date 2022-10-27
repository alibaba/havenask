#ifndef ISEARCH_BS_CONFIGDOWNLOADER_H
#define ISEARCH_BS_CONFIGDOWNLOADER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <fslib/fslib.h>

namespace build_service {
namespace task_base {

class ConfigDownloader
{
public:
    enum DownloadErrorCode {
        DEC_NONE = 0,
        DEC_NORMAL_ERROR = 1,
        DEC_DEST_ERROR = 2
    };
public:
    ConfigDownloader();
    ~ConfigDownloader();
private:
    ConfigDownloader(const ConfigDownloader &);
    ConfigDownloader& operator=(const ConfigDownloader &);
public:
    static DownloadErrorCode downloadConfig(
            const std::string &remotePath, std::string &localPath);
    static std::string getLocalConfigPath(const std::string &remotePath,
            const std::string &localPath);
private:
    static bool isLocalFile(const std::string &path);
    static DownloadErrorCode doDownloadConfig(
            const std::string &remotePath, const std::string &localPath);
private:
    static DownloadErrorCode removeIfExist(const std::string &localPath);
    static bool isLocalDiskError(fslib::ErrorCode ec);
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ConfigDownloader);

}
}

#endif //ISEARCH_BS_CONFIGDOWNLOADER_H
