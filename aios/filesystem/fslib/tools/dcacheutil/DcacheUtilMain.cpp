/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <iostream>
#include <errno.h>
#include <stdio.h>
#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/ThreadPool.h"
#include "autil/legacy/jsonizable.h"
#include "tools/fsutil/FsUtil.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/common/common_define.h"
#include "kmonitor/client/common/Util.h"

#include <sys/types.h>

using namespace std;
using namespace fslib;
FSLIB_USE_NAMESPACE(tools);

AUTIL_DECLARE_AND_SETUP_LOGGER(tools, dcacheutil);

const string DEFAULT_LOG_CONF =
    "alog.rootLogger=INFO, fslibAppender\n"
    "alog.appender.fslibAppender=FileAppender\n"
    "alog.appender.fslibAppender.fileName=/tmp/dcache_util.log\n"
    "alog.appender.fslibAppender.flush=true\n"
    "alog.appender.fslibAppender.max_file_size=100\n"
    "alog.appender.fslibAppender.layout=PatternLayout\n"
    "alog.appender.fslibAppender.layout.LogPattern=[\%\%d] [\%\%l] [\%\%t,\%\%F -- \%\%f():\%\%n] [\%\%m]\n"
    "alog.appender.fslibAppender.log_keep_count=10";

struct DcacheConf {
    string dcacheServer;
    string md5;
    string idc;
    string cmPath;
    string cluster;
    bool warmup;
    bool bOverride;
    bool checkCache;
    int64_t speed;
    DcacheConf() {
        speed = 0;
        warmup = true;
        bOverride = false;
        checkCache = true;
        dcacheServer = "bin_dcache";
    }
};

class BatchCopyItem : public autil::legacy::Jsonizable {
public:
    BatchCopyItem() {};
    BatchCopyItem(string src, string dst, string md5);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    
public:
    string src;
    string dst;
    string md5;
};

BatchCopyItem::BatchCopyItem(string src, string dst, string md5) : src(src), dst(dst), md5(md5) {
}

void BatchCopyItem::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("src", src);
    json.Jsonize("dst", dst);
    json.Jsonize("md5", md5, md5);
}

void printDcacheUtilCmdUsage() {
    cout << endl
         << "usage:" << endl
         << "  " << FsUtil::COPY_CMD << " [-r] srcname dstname      "
         << "copy file or dir from srcname to dstname, for copy dir,"
         << " -r is needed" << endl
         << "  " << FsUtil::LISTDIR_CMD << " dirname [-R] [threadNum]  "
         << "list files under the directory, "
         << "-R means list dir recurisively" << endl
         << "  " << FsUtil::GETMETA_CMD << " pathname             "
         << "get meta info of file or directory" << endl
         << "  " << FsUtil::CAT_CMD << " filename                 "
         << "ouput the content of the file" << endl
         << "  " << FsUtil::ISEXIST_CMD << " pathname             "
         << "check whether the pathname exist" << endl
         << " --cmPath zfs://xx, cmServer path" << endl
         << " --md5 xxx, enable md5sum check, only support copy one file" << endl
         << " --checkcache true/false, check cache by modifytime&length, default true" << endl
         << " --warmup true, warmup file to accelerate code file, default true" << endl
         << " --cluster HIPPO_NA61_7U, hippo cluster, default from HIPPO_SERVICE_NAME env" << endl
         << " --dcacheServer bin_dcache, spec dcacheServer, multi dcacheServers in one hippo cluster, default bin_dcache" << endl
         << " --speedMB 10, download speed limit, default and suggest unlimited" << endl
         << " --override true, override file when dstFile exist, default false " << endl
         << "  -l log conf file             "
         <<"optional, log conf file. default: Console, WARN" << endl
         << "  --defaultLog logLevel        "
         <<"optional, whether use default log and set default log level" << endl
         <<endl
         <<"  example:"<<endl
         <<"  dcache_util ls /path/to/dir/" <<endl
         <<"  dcache_util cp /path/srcfile /localpath/dstfile" <<endl
         <<"  dcache_util cp 'http://srcfile' /localpath/dstfile --cmPath 'zfs://xxx' --md5 xxx" <<endl
         <<"  dcache_util cp 'dfs://srcfile' /localpath/dstfile --cluster HIPPO_PRE_7U --dcacheServer bin_dcache" <<endl
         <<"  dcache_util cp 'pangu://srcfile' /localpath/dstfile -l /path/to/log/alog.conf --speedMB 50" << endl
         <<"  dcache_util ls /path/to/dir/ --defaultLog INFO" << endl
         <<"  dcache_util batchCp '[{\"src\":\"pangu://srcfile1\",\"dst\":\"/localpath/dstfile1\"},{\"src\":\"pangu://srcfile2\",\"dst\":\"/localpath/dstfile2\"}]' -l /path/to/log/alog.conf --speedMB 50" << endl
         <<endl;
}

static const std::string kOrgFsTypeSep = "://";
static const std::string kEncodedFsTypeSep = "/";
static const std::string kDCacheFsPrefix = "dcache://";
std::string convertToDcacheUrl(const char* path, const DcacheConf &conf) {
    string url(path);
    if (conf.dcacheServer.empty()) {
        return url.c_str();
    }
    auto pos = url.find(kOrgFsTypeSep);
    if (std::string::npos == pos) {
        return url.c_str();
    }
    if (autil::StringUtil::startsWith(url, kDCacheFsPrefix)) {
        printf("no need format url[%s] to dcache again\n", url.c_str());
        return url.c_str();
    }
    auto fsType = url.substr(0, pos);
    auto filePath = url.substr(pos + kOrgFsTypeSep.length());
    url = kDCacheFsPrefix + conf.dcacheServer + kEncodedFsTypeSep + fsType + kEncodedFsTypeSep + filePath;
    return url;
}

bool warmUpRemote(vector<BatchCopyItem> items, const DcacheConf &conf) {
    ErrorCode ret = EC_OK;
    //按单文件预热可以让冷数据下载更快，但是会导致预热rpc请求量过多，所以目前是合并一次性
    for (auto it = items.begin(); it != items.end(); ++it) {
        auto path = convertToDcacheUrl(it->src.c_str(), conf);
        if(!fs::FileSystem::isFile(path)) {
            continue;
        }
        FileMeta fileMeta;
        ret = fs::FileSystem::getFileMeta(path, fileMeta);
        if (ret != EC_OK) {
            continue;
        }
        if (fileMeta.fileLength == 0) {
            continue;
        }

        //由于forward是外层查的长度，所以目前先做在这里替换，更合理的是统一在forward中，外层不传length，但之前协议已经存在了，所以暂时不动
        stringstream arg;
        if (conf.checkCache && fileMeta.lastModifyTime > 0) {
            string oldDcachePrefix = kDCacheFsPrefix + conf.dcacheServer;
            string newDcachePrefix = kDCacheFsPrefix + conf.dcacheServer + "?modifytime="+std::to_string(fileMeta.lastModifyTime) + "&length="+std::to_string(fileMeta.fileLength);
            string metaPath = path;
            metaPath = newDcachePrefix + metaPath.substr(oldDcachePrefix.length(), std::string::npos);
            arg << metaPath << ":::" << std::to_string(fileMeta.fileLength) << "\n";
        } else {
            arg << path << ":::" << std::to_string(fileMeta.fileLength) << "\n";
        }

        string forwardPath = kDCacheFsPrefix + conf.dcacheServer;
        string output;
        ret = fs::FileSystem::forward("ServerLoad", forwardPath, arg.str(), output);
        if (ret != EC_OK) {
            cerr << "send warmup request failed" << "retCode[" << ret << "] output[" << output << "]" <<  endl;
            return false;
        }
    }
    

    return true;
}

void readStream(FILE* stream, string *pattern, int32_t timeout) {
    int64_t expireTime = autil::TimeUtility::currentTime() + timeout * 1000000;
    char buf[256];
    while (autil::TimeUtility::currentTime() < expireTime) {
        size_t len = fread(buf, sizeof(char), 256, stream);
        if (ferror(stream)) {
            *pattern = "";
            return;
        }

        *pattern += string(buf, len);

        if (feof(stream)) {
            break;
        }
    }
}

int32_t call(const std::string &command, std::string *out, int32_t timeout)
{
    FILE *stream = popen(command.c_str(), "r");
    if (stream == NULL) {
        return -1;
    }
    out->clear();
    readStream(stream, out, timeout);
    int32_t ret = pclose(stream);
    if (out->length() > 0 && *out->rbegin() == '\n') {
        *out = out->substr(0, out->length() - 1);
    }
    if (WIFEXITED(ret) != 0) {
        return WEXITSTATUS(ret);
    }
    return -1;
}

//目前简单实现，从dp2 copy过来的，可能走md5库更合适
std::string getFileMd5(const std::string &filePath) {
    string cmd = string("/usr/bin/md5sum ") + filePath + string("| awk '{print $1}'");
    string md5;
    if (call(cmd, &md5, 20) != 0) {
        printf("get md5 failed, cmd: [%s], output:[%s].\n", cmd.c_str(), md5.c_str());
        return  "";
    }
    return md5;
}

ErrorCode runCopy(const DcacheConf &conf, const BatchCopyItem& copy, bool recursive = false) {
    ErrorCode ret = EC_UNKNOWN;
    auto srcDcachePath = convertToDcacheUrl(copy.src.c_str(), conf);
    auto begin = autil::TimeUtility::currentTime();
    ret = FsUtil::runCopy(srcDcachePath.c_str(), copy.dst.c_str(), recursive);
    AUTIL_LOG(INFO, "copy src[%s] to dst[%s] cost:%ldms",
              copy.src.c_str(), copy.dst.c_str(), (autil::TimeUtility::currentTime() - begin) / 1000);
    
    if (ret != EC_OK) {
        cerr << "failed to copy " << copy.src << " to " << copy.dst << ", ret:" << ret << endl; 
        return ret;
    }
    if (!copy.md5.empty()) {
        string curMd5 = getFileMd5(copy.dst);
        if (copy.md5 != curMd5) {
            cerr << "bad file for md5 check cur[" << curMd5 <<"] != target[" << copy.md5 << "]" << endl;
            return EC_BADF;
        }
    }
    return ret;
}

ErrorCode processDcacheCmd(int argc, char** argv, DcacheConf &conf) {
    ErrorCode ret = EC_UNKNOWN;
    auto begin = autil::TimeUtility::currentTime();
    if (argc >= 2) {
        if (strcmp(argv[1], FsUtil::COPY_CMD.c_str()) == 0) {
            bool recursive = false;
            char* srcPath = NULL;
            char* dstPath = NULL;
            if (argc >= 5 && strcmp(argv[2], "-r") == 0) {
                recursive = true;
                srcPath = argv[3];
                dstPath = argv[4];
            } else if (argc >= 4) {
                recursive = false;
                srcPath = argv[2];
                dstPath = argv[3];
            } else {
                return ret;
            }
            BatchCopyItem copyItem(srcPath, dstPath, conf.md5);
            if (conf.warmup) {
                warmUpRemote(vector<BatchCopyItem>{copyItem}, conf);
            }
            ret = runCopy(conf, copyItem, recursive);
            return ret;
        } else if (strcmp(argv[1], "batchCp") == 0) {
            bool recursive = false;
            char* itemsStr = NULL;
            if (argc >= 4 && strcmp(argv[2], "-r") == 0) {
                recursive = true;
                itemsStr = argv[3];
            } else if (argc >= 3) {
                recursive = false;
                itemsStr = argv[2];
            } else {
                return ret;
            }
            vector<BatchCopyItem> items;
            try {
                FromJsonString(items, itemsStr);
            } catch (const autil::legacy::ExceptionBase& e) {
                cerr << "parse batch copy items err:" << e.GetMessage().c_str() << endl;
                return EC_BADARGS;
            }
            
            autil::ThreadPool threadPool;
            threadPool.start("download");
            if (conf.warmup) {
                auto task = [&items, &conf]() {
                                warmUpRemote(items, conf);
                            };
                if (autil::ThreadPool::ERROR_NONE != threadPool.pushTask(task)) {
                    warmUpRemote(items, conf);
                }
            }

            for (auto it = items.begin(); it != items.end(); ++it) {
                auto task = [&ret, &conf, it, recursive]() {
                                auto curRet = runCopy(conf, *it, recursive);
                                if (curRet != EC_OK) {
                                    cerr << "failed to get " << it->src << " ret:"<< ret << endl;
                                    ret = curRet;
                                }
                            };
                if (autil::ThreadPool::ERROR_NONE != threadPool.pushTask(task)) {
                    ret = runCopy(conf, *it, recursive);
                    if (ret != EC_OK) {
                        return ret;
                    }
                }
            }
            threadPool.waitFinish();
            AUTIL_LOG(INFO, "batch copy %s cost:%ldms", itemsStr, (autil::TimeUtility::currentTime() - begin) / 1000);
            return ret;
        } else if (strcmp(argv[1], FsUtil::LISTDIR_CMD.c_str()) == 0) {
            if (argc == 3) {
                return FsUtil::runListDir(convertToDcacheUrl(argv[2], conf).c_str());
            } else if (argc == 4) {
                if (strcmp(argv[3], "-R") == 0){
                    return FsUtil::runListDirWithRecursive(convertToDcacheUrl(argv[2], conf).c_str(), "");
                } else if (strcmp(argv[2], "-R") == 0){
                    return FsUtil::runListDirWithRecursive(convertToDcacheUrl(argv[3], conf).c_str(), "");
                }
            } else if (argc == 5){
                if (strcmp(argv[3], "-R") == 0) {
                    return FsUtil::runListDirWithRecursive(convertToDcacheUrl(argv[2], conf).c_str(), argv[4]);
                } else if (strcmp(argv[2], "-R") == 0){
                    return FsUtil::runListDirWithRecursive(convertToDcacheUrl(argv[4], conf).c_str(), argv[3]);
                }
            } 
        } else if (strcmp(argv[1], FsUtil::CAT_CMD.c_str()) == 0 &&
                   argc == 3) 
        {
            return FsUtil::runCat(convertToDcacheUrl(argv[2], conf).c_str());
        } else if (strcmp(argv[1], FsUtil::GETMETA_CMD.c_str()) == 0 &&
                   argc == 3) 
        {
            return FsUtil::runGetMeta(convertToDcacheUrl(argv[2], conf).c_str());
        } else if (strcmp(argv[1], FsUtil::ISEXIST_CMD.c_str()) == 0 &&
                   argc == 3) 
        {
            return FsUtil::runIsExist(convertToDcacheUrl(argv[2], conf).c_str());
        }
    }
    
    printDcacheUtilCmdUsage();
    return EC_BADARGS;
}

bool initLogWithLogFile(const string &logConf) {
    if (access(logConf.c_str(), F_OK) == 0) {
        AUTIL_LOG_CONFIG(logConf.c_str());
        return true;
    } else {
        cerr << "WARN: Can't access log conf file[" << logConf
             << "],use default log conf." << endl;
        return false;
    }    
}

void initLogWithDefaultConf(const string &defaultLogLevel) {
    try {
        alog::Configurator::configureLoggerFromString(DEFAULT_LOG_CONF.c_str());
        
        switch (defaultLogLevel[0]) {
        case 'D': 
        case 'd': {
            AUTIL_ROOT_LOG_SETLEVEL(DEBUG);
            break;
        }
        case 'I': 
        case 'i': {
            AUTIL_ROOT_LOG_SETLEVEL(INFO);
            break;
        }
        case 'W': 
        case 'w': {
            AUTIL_ROOT_LOG_SETLEVEL(WARN);
            break;
        }
        case 'E': 
        case 'e': {
            AUTIL_ROOT_LOG_SETLEVEL(ERROR);
            break;
        }         
        case 'F': 
        case 'f': {
            AUTIL_ROOT_LOG_SETLEVEL(FATAL);
            break;
        }
        default: {
            AUTIL_ROOT_LOG_SETLEVEL(WARN);
            break;
        }
        }
    } catch(std::exception &e) {
        std::cerr << "WARN! Failed to configure logger!"
                  << e.what() << ",use default log conf."
                  << std::endl;
        AUTIL_ROOT_LOG_CONFIG();
    }
}

void initLog(int &argc ,char** argv) {
    string logConf;
    string defaultLogLevel = "INFO";
    if (argc >= 4) {
        for (int i = 1; i < argc - 1;) {
            if (strcmp(argv[i], "-l") == 0 
                || strcmp(argv[i], "--log") == 0) 
            {
                logConf = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--defaultLog") == 0) {
                defaultLogLevel = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else {
                i += 1;
            }
        }
    }
    if (!logConf.empty() && initLogWithLogFile(logConf)) {
        return;
    }

    if (!defaultLogLevel.empty()) {
        initLogWithDefaultConf(defaultLogLevel);
        return;
    }

    AUTIL_ROOT_LOG_CONFIG();
    AUTIL_ROOT_LOG_SETLEVEL(WARN);
}

void initDcacheConfig(int &argc ,char** argv, DcacheConf &conf) {
    if (argc >= 4) {
        for (int i = 1; i < argc - 1;) {
            if (strcmp(argv[i], "--cmPath") == 0) 
            {
                conf.cmPath = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--md5") == 0) {
                conf.md5 = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--warmup") == 0) {
                string warmupStr = string(argv[i + 1]);
                autil::StringUtil::toLowerCase(warmupStr);
                if (warmupStr == "true") {
                    conf.warmup = true;
                } else {
                    conf.warmup = false;
                }
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--cluster") == 0) {
                conf.cluster = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--dcacheServer") == 0) {
                conf.dcacheServer = string(argv[i + 1]);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--speedMB") == 0) {
                autil::StringUtil::fromString(argv[i + 1], conf.speed);
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--override") == 0) {
                string overrideStr = string(argv[i + 1]);
                autil::StringUtil::toLowerCase(overrideStr);
                if (overrideStr == "true") {
                    conf.bOverride = true;
                } else {
                    conf.bOverride = false;
                }
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else if (strcmp(argv[i], "--checkcache") == 0) {
                string overrideStr = string(argv[i + 1]);
                autil::StringUtil::toLowerCase(overrideStr);
                if (overrideStr == "true") {
                    conf.checkCache = true;
                } else {
                    conf.checkCache = false;
                }
                for (int j = i; j < argc - 2; ++j) {
                    argv[j] = argv[j + 2];
                }
                argc -= 2;
            } else {
                i += 1;
            }
        }
    }

    if (!conf.cmPath.empty()) {
        ::setenv("DCACHE_CM_SERVER_PATH", conf.cmPath.c_str(), 0);
    }
    if (!conf.dcacheServer.empty()) {
        ::setenv("DCACHE_SERVER", conf.dcacheServer.c_str(), 0);
    }
    if (!conf.cluster.empty()) {
        ::setenv("HIPPO_SERVICE_NAME", conf.cluster.c_str(), 1); //这个环境变量本身可能得考虑替换掉
    }
    if (conf.speed > 0) {
        stringstream speedStr;
        cout << "set speed to:" << conf.speed << endl; 
        speedStr << "quota_size_in_byte=" << conf.speed * 1024 * 1024;
        speedStr << ",quota_interval_in_ms=1000";
        ::setenv("FSLIB_READ_SPEED_LIMIT", speedStr.str().c_str(), 0);
    }
    if (conf.bOverride) {
        ::setenv(FSLIB_COPY_IS_OVERRIDE, "true", 0);
    }
    if (conf.checkCache) {
        ::setenv(FSLIB_META_CHECK_CACHE, "true", 0);
    }

    char* blockSizeEnv = ::getenv("FSLIB_COPY_BLOCK_SIZE");
    if (NULL == blockSizeEnv) {
        ::setenv("FSLIB_COPY_BLOCK_SIZE", "8388608", 0); //dcache 需要8M，业务通常不自己设置
    }
    
    if (NULL == ::getenv("HIPPO_SLAVE_IP")) { //统计需要
        string host;
        if(kmonitor::Util::GetHostIP(host)) {
            ::setenv("HIPPO_SLAVE_IP", host.c_str(), 0); 
        }
    }

    if (NULL == ::getenv("DCACHE_CONNECTION_TIMEOUT")) { //默认超时时间改小
        ::setenv("DCACHE_CONNECTION_TIMEOUT", "4000", 0); //4S，太小可能pangu都还没回数据
    }
}

int main(int argc, char** argv)
{
    int ret = 0;
    DcacheConf conf;
    initLog(argc, argv);
    initDcacheConfig(argc, argv, conf);
    ret = processDcacheCmd(argc, argv, conf);
    std::cout.flush();
    std::cerr.flush();
    const char* exitMode = getenv("FSUTIL_NORMAL_EXIT");
    if (!exitMode) {
        _exit(ret);
    } else if (strcmp(exitMode, "close") == 0) {
        FsUtil::closeFileSystem();
    }
    return ret;
}
