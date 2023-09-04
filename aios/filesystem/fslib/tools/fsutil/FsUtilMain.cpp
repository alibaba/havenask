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
#include <errno.h>
#include <iostream>

#include "autil/EnvUtil.h"
#include "fslib/config.h"
#include "tools/fsutil/FsUtil.h"

using namespace std;
using namespace fslib;
FSLIB_USE_NAMESPACE(tools);

const string DEFAULT_LOG_CONF =
    "alog.rootLogger=INFO, fslibAppender\n"
    "alog.appender.fslibAppender=FileAppender\n"
    "alog.appender.fslibAppender.fileName=fs_util.log\n"
    "alog.appender.fslibAppender.async_flush=true\n"
    "alog.appender.fslibAppender.flush_threshold=1024\n"
    "alog.appender.fslibAppender.flush_interval=1000\n"
    "alog.appender.fslibAppender.max_file_size=100\n"
    "alog.appender.fslibAppender.layout=PatternLayout\n"
    "alog.appender.fslibAppender.layout.LogPattern=[\%\%d] [\%\%l] [\%\%t,\%\%F -- \%\%f():\%\%n] [\%\%m]\n"
    "alog.appender.fslibAppender.log_keep_count=100";

void printFsUtilCmdUsage() {
    cout << endl
         << "usage:" << endl
         << "  " << FsUtil::COPY_CMD << " [-r] srcname dstname      "
         << "copy file or dir from srcname to dstname, for copy dir,"
         << " -r is needed" << endl
         << "  " << FsUtil::MOVE_CMD << " srcname dstname           "
         << "move file or dir from srcname to dstname" << endl
         << "  " << FsUtil::MKDIR_CMD << " [-p] dirname           "
         << "create a directory, if any parent dir of dirname does not exist,"
         << " -p is needed" << endl
         << "  " << FsUtil::LISTDIR_CMD << " dirname [-R] [threadNum]  "
         << "list files under the directory, "
         << "-R means list dir recurisively" << endl
         << "  " << FsUtil::REMOVE_CMD << " pathname                  "
         << "remove file or directory" << endl
         << "  " << FsUtil::GETMETA_CMD << " pathname             "
         << "get meta info of file or directory" << endl
         << "  " << FsUtil::CAT_CMD << " filename                 "
         << "ouput the content of the file" << endl
         << "  " << FsUtil::ZCAT_CMD << " filename                "
         << "ouput the content of the gz file" << endl
         << "  " << FsUtil::MD5_SUM_CMD << " filename              "
         << "ouput the md5 sum of the file" << endl
         << "  " << FsUtil::ISEXIST_CMD << " pathname             "
         << "check whether the pathname exist" << endl
         << "  " << FsUtil::RENAME_CMD << " srcname dstname       "
         << "rename file or dir from srcname to dstname" << endl
         << "  " << FsUtil::FLOCK_CMD << " lockname script        "
         << "use file with name lockname as lock, lock it first, then run script" << endl
         << "  " << FsUtil::FORWARD_CMD << " cmd pathname args    " << endl
         << "  " << FsUtil::ENCRYPT_CMD << " srcfile dstfile [cipherOption] [--base64]     "
         << "encrypt file from srcPath to dstPath, dst file could be decripted by openssl" << endl
         << "  " << FsUtil::DECRYPT_CMD << " srcfile dstfile [cipherOption] [--base64]     "
         << "decrypt file from srcPath to dstPath, src file could be encripted file by openssl" << endl
         << "  " << FsUtil::DECRYPT_CAT_CMD << " file cipherOption [--base64]     "
         << "output content of the encrypted file" << endl
         << "  " << FsUtil::DECRYPT_ZCAT_CMD << " file cipherOption [--base64]    "
         << "output content of the encrypted gz file" << endl
         << "custom behavior of each file system" << endl
         << "  -l log conf file             "
         << "optional, log conf file. default: Console, WARN" << endl
         << "  --defaultLog logLevel        "
         << "optional, whether use default log and set default log level" << endl
         << endl
         << "  example:" << endl
         << "  fs_util ls /path/to/dir/" << endl
         << "  fs_util ls /path/to/dir/ -R" << endl
         << "  fs_util ls /path/to/dir/ -R 16" << endl
         << "  fs_util ls /path/to/dir/ -l /path/to/log/conf/file" << endl
         << "  fs_util ls /path/to/dir/ --defaultLog INFO" << endl
         << "  fs_util cat  /path/file" << endl
         << "  fs_util zcat /path/file.gz" << endl
         << "  fs_util encrypt /path/raw_file /path/enc_file \'type=aes-128-cbc;digist=md5;passwd=enl3MDA3\' --base64 "
         << endl
         << "  fs_util decrypt /path/enc_file /path/dec_file \'type=aes-128-cbc;digist=md5;passwd=password_term\' "
         << endl
         << "  fs_util dcat  /path/enc_file    \'type=aes-128-cbc;digist=md5;passwd=password_term\' " << endl
         << "  fs_util dzcat /path/enc_file.gz \'type=aes-128-cbc;digist=md5;passwd=password_term\' " << endl;
}

ErrorCode processCmd(int argc, char **argv) {
    if (argc >= 2) {
        if (strcmp(argv[1], FsUtil::COPY_CMD.c_str()) == 0) {
            if (argc == 5 && strcmp(argv[2], "-r") == 0) {
                return FsUtil::runCopy(argv[3], argv[4], true);
            } else if (argc == 4) {
                return FsUtil::runCopy(argv[2], argv[3], false);
            }
        } else if (strcmp(argv[1], FsUtil::MOVE_CMD.c_str()) == 0 && argc == 4) {
            return FsUtil::runMove(argv[2], argv[3]);
        } else if (strcmp(argv[1], FsUtil::RENAME_CMD.c_str()) == 0 && argc == 4) {
            return FsUtil::runRename(argv[2], argv[3]);
        } else if (strcmp(argv[1], FsUtil::MKDIR_CMD.c_str()) == 0) {
            if (argc == 4 && strcmp(argv[2], "-p") == 0) {
                return FsUtil::runMkDir(argv[3], true);
            } else if (argc == 3) {
                return FsUtil::runMkDir(argv[2], false);
            }
        } else if (strcmp(argv[1], FsUtil::LISTDIR_CMD.c_str()) == 0) {
            if (argc == 3) {
                return FsUtil::runListDir(argv[2]);
            } else if (argc == 4) {
                if (strcmp(argv[3], "-R") == 0) {
                    return FsUtil::runListDirWithRecursive(argv[2], "");
                } else if (strcmp(argv[2], "-R") == 0) {
                    return FsUtil::runListDirWithRecursive(argv[3], "");
                }
            } else if (argc == 5) {
                if (strcmp(argv[3], "-R") == 0) {
                    return FsUtil::runListDirWithRecursive(argv[2], argv[4]);
                } else if (strcmp(argv[2], "-R") == 0) {
                    return FsUtil::runListDirWithRecursive(argv[4], argv[3]);
                }
            }
        } else if (strcmp(argv[1], FsUtil::REMOVE_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runRemove(argv[2]);
        } else if (strcmp(argv[1], FsUtil::CAT_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runCat(argv[2]);
        } else if (strcmp(argv[1], FsUtil::MD5_SUM_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runMd5Sum(argv[2]);
        } else if (strcmp(argv[1], FsUtil::ZCAT_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runZCat(argv[2]);
        } else if (strcmp(argv[1], FsUtil::GETMETA_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runGetMeta(argv[2]);
        } else if (strcmp(argv[1], FsUtil::ISEXIST_CMD.c_str()) == 0 && argc == 3) {
            return FsUtil::runIsExist(argv[2]);
        } else if (strcmp(argv[1], FsUtil::FLOCK_CMD.c_str()) == 0 && argc == 4) {
            return FsUtil::runFlock(argv[2], argv[3]);
        } else if (strcmp(argv[1], FsUtil::FORWARD_CMD.c_str()) == 0 && (argc == 4 || argc == 5)) {
            return FsUtil::runForward(argv[2], argv[3], argc == 4 ? "" : argv[4]);
        } else if (strcmp(argv[1], FsUtil::ENCRYPT_CMD.c_str()) == 0) {
            if (argc == 4) {
                return FsUtil::runEncrypt(argv[2], argv[3], nullptr, false);
            }
            if (argc == 5) {
                return FsUtil::runEncrypt(argv[2], argv[3], argv[4], false);
            }
            if (argc == 6 && strcmp(argv[5], "--base64") == 0) {
                return FsUtil::runEncrypt(argv[2], argv[3], argv[4], true);
            }
        } else if (strcmp(argv[1], FsUtil::DECRYPT_CMD.c_str()) == 0) {
            if (argc == 4) {
                return FsUtil::runDecrypt(argv[2], argv[3], nullptr, false);
            }
            if (argc == 5) {
                return FsUtil::runDecrypt(argv[2], argv[3], argv[4], false);
            }
            if (argc == 6 && strcmp(argv[5], "--base64") == 0) {
                return FsUtil::runDecrypt(argv[2], argv[3], argv[4], true);
            }
        } else if (strcmp(argv[1], FsUtil::DECRYPT_CAT_CMD.c_str()) == 0) {
            if (argc == 3) {
                return FsUtil::runDecryptCat(argv[2], nullptr, false);
            }
            if (argc == 4) {
                return FsUtil::runDecryptCat(argv[2], argv[3], false);
            }
            if (argc == 5 && strcmp(argv[4], "--base64") == 0) {
                return FsUtil::runDecryptCat(argv[2], argv[3], true);
            }
        } else if (strcmp(argv[1], FsUtil::DECRYPT_ZCAT_CMD.c_str()) == 0) {
            if (argc == 3) {
                return FsUtil::runDecryptZCat(argv[2], nullptr, false);
            }
            if (argc == 4) {
                return FsUtil::runDecryptZCat(argv[2], argv[3], false);
            }
            if (argc == 5 && strcmp(argv[4], "--base64") == 0) {
                return FsUtil::runDecryptZCat(argv[2], argv[3], true);
            }
        }
    }

    printFsUtilCmdUsage();
    return EC_BADARGS;
}

bool initLogWithLogFile(const string &logConf) {
    if (access(logConf.c_str(), F_OK) == 0) {
        AUTIL_LOG_CONFIG(logConf.c_str());
        return true;
    } else {
        cerr << "WARN: Can't access log conf file[" << logConf << "],use default log conf." << endl;
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
    } catch (std::exception &e) {
        std::cerr << "WARN! Failed to configure logger!" << e.what() << ",use default log conf." << std::endl;
        AUTIL_ROOT_LOG_CONFIG();
    }
}

void initLog(int &argc, char **argv) {
    string logConf;
    string defaultLogLevel;
    if (argc >= 4) {
        for (int i = 1; i < argc - 1;) {
            if (strcmp(argv[i], "-l") == 0 || strcmp(argv[i], "--log") == 0) {
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

int main(int argc, char **argv) {
    int ret = 0;
    initLog(argc, argv);
    ret = processCmd(argc, argv);
    std::cout.flush();
    std::cerr.flush();
    AUTIL_LOG_FLUSH();
    /* not clean up pangu's gloal objects
       although apsara 0.14 has fix this problem, it still takes >3 secs at clean up while cat pangu files.
       use _exit to speed up clean up process.
     */
    string exitMode = autil::EnvUtil::getEnv("FSUTIL_NORMAL_EXIT");
    if (exitMode.empty()) {
        _exit(ret);
    } else if (exitMode == "close") {
        FsUtil::closeFileSystem();
    }
    return ret;
}
