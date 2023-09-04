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
#include "build_service/tools/remote_logger/RemoteLogger.h"

#include <fcntl.h>
#include <sys/stat.h>

#include "autil/NetUtil.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/IndexPathConstructor.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/URLParser.h"
using fslib::fs::FileSystem;

namespace build_service { namespace tools {
AUTIL_LOG_SETUP(log_uploader, RemoteLogger);

RemoteLogger::RemoteLogger()
    : _outputFile(nullptr)
    , _remoteFileIdx(0)
    , _currentFileLength(0)
    , _sourceFd(-1)
    , _stopped(false)
    , _startTime(0)
    , _lastCleanExpiredLogSecond(-1)
    , _lastFileCreateSecond(-1)
{
}

RemoteLogger::~RemoteLogger() {}

fslib::fs::File* RemoteLogger::genRemoteFile(int64_t keepLogHour)
{
    std::string ip;
    int64_t currentSecond = autil::TimeUtility::currentTimeInSeconds();
    if (!autil::NetUtil::GetDefaultIp(ip)) {
        AUTIL_LOG(ERROR, "get ip failed");
        return nullptr;
    }
    if (currentSecond - _lastCleanExpiredLogSecond >= 300) {
        AUTIL_LOG(INFO, "try clean expired log files");
        cleanExpiredLogs(_remoteLogDir, keepLogHour);
        _lastCleanExpiredLogSecond = currentSecond;
    }
    std::stringstream ss;
    ss << ip << "_" << _startTime << "_bs.log_" << _remoteFileIdx++;
    std::string remoteFileName = FileSystem::joinFilePath(_remoteLogDir, ss.str());
    fslib::fs::File* remoteFile = FileSystem::openFile(remoteFileName, fslib::Flag::WRITE);
    AUTIL_LOG(INFO, "generate remote file [%s], ptr[%p]", remoteFileName.c_str(), remoteFile);
    _lastFileCreateSecond = currentSecond;
    return remoteFile;
}

bool RemoteLogger::initPid()
{
    std::string cwd = _TEST_cwd;
    if (cwd.empty()) {
        char cwdPath[PATH_MAX];
        char* ret = getcwd(cwdPath, PATH_MAX);
        if (!ret) {
            AUTIL_LOG(ERROR, "get cwd failed");
            return false;
        }
        cwd = std::string(cwdPath);
    }
    AUTIL_LOG(INFO, "cwd [%s]", cwd.c_str());

    // /slave/disk_links/1/bs_biz_order_his_na630_hdd_compare_xiaohao.biz_order_summary.999.builder.full.32768.49151.biz_order_summary_34_72/log_uploader
    auto rolePaths = autil::StringUtil::split(cwd, "/");

    if (rolePaths.size() < 2) {
        AUTIL_LOG(ERROR, "splited path size is less than 2");
        return false;
    }
    std::string rolePath = rolePaths[rolePaths.size() - 2];
    std::string role = extractRoleFromPath(rolePath);
    std::string appName = autil::StringUtil::split(role, ".")[0];
    if (!proto::ProtoUtil::roleIdToPartitionId(role, appName, _pid)) {
        AUTIL_LOG(ERROR, "get pid from role [%s], appName [%s] failed", role.c_str(), appName.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "init pid [%s] success ", _pid.ShortDebugString().c_str());
    return true;
}

std::string RemoteLogger::extractRoleFromPath(const std::string& rolePath)
{
    // hippo path: app, role, len(app), len(role)
    // local agent path: role
    auto detailPath = autil::StringUtil::split(rolePath, "_");
    size_t detailLen = detailPath.size();
    int32_t appLen = 0;
    int32_t roleLen = 0;
    if (detailLen > 2 && autil::StringUtil::fromString(detailPath[detailLen - 2], appLen) &&
        autil::StringUtil::fromString(detailPath[detailLen - 1], roleLen)) {
        return rolePath.substr(appLen + 1, roleLen);
    }
    return rolePath;
}

int32_t RemoteLogger::openFifoFile(const std::string& fileName)
{
    struct stat file_stat;

    // Check if file exists
    while (stat(fileName.c_str(), &file_stat) != 0) {
        if (_stopped) {
            AUTIL_LOG(ERROR, "source file not exist but receive stop signal");
            return -1;
        }
        // Wait for file to be created by another process
        AUTIL_LOG(INFO, " pipe file [%s] not exit, waiting...", fileName.c_str());
        usleep(1000000); // sleep for 1 second
    }

    // Check if file is a named pipe (mkfifo)
    if (S_ISFIFO(file_stat.st_mode)) {
        // Open file in read-only mode
        int32_t fd = open(fileName.c_str(), O_RDONLY);
        if (fd == -1) {
            AUTIL_LOG(ERROR, "Failed to open file [%s], error [%s]", fileName.c_str(), strerror(errno));
            return -1;
        }
        AUTIL_LOG(INFO, "open fifo file success, fd [%d]", fd);
        return fd;
    } else {
        AUTIL_LOG(ERROR, " file [%s] is not a named pipe", fileName.c_str());
        return -1;
    }
}

bool RemoteLogger::calculateRemoteDir()
{
    while (!_stopped.load()) {
        usleep(1 * 1000 * 1000); // 1s
        auto configPath = FileSystem::joinFilePath(_bsWorkDir, "config");
        if (fslib::EC_TRUE != FileSystem::isExist(configPath)) {
            AUTIL_LOG(INFO, "waiting config path [%s]", configPath.c_str());
            continue;
        }
        fslib::FileList dirs;
        if (fslib::EC_OK != FileSystem::listDir(configPath, dirs)) {
            AUTIL_LOG(WARN, "list config path [%s] failed", configPath.c_str());
            continue;
        }
        for (const auto& dir : dirs) {
            std::string singleConfigPath = FileSystem::joinFilePath(configPath, dir);
            config::ResourceReader resourceReader(singleConfigPath);
            std::string indexRoot;
            if (!resourceReader.getConfigWithJsonPath("build_app.json", "index_root", indexRoot)) {
                AUTIL_LOG(INFO, "get index root from config path [%s] failed, ignore", singleConfigPath.c_str());
                continue;
            }
            std::vector<std::string> allClusters;
            if (!resourceReader.getAllClusters(_pid.buildid().datatable(), allClusters) || allClusters.empty()) {
                AUTIL_LOG(INFO, "get all clusters failed");
                continue;
            }
            indexRoot = fslib::util::URLParser::getPurePath(indexRoot);
            _remoteLogDir = util::IndexPathConstructor::getGenerationIndexPath(indexRoot, allClusters[0],
                                                                               _pid.buildid().generationid());
            _remoteLogDir = FileSystem::joinFilePath(_remoteLogDir, "logs");
            _remoteLogDir = FileSystem::joinFilePath(_remoteLogDir, proto::ProtoUtil::toRoleString(_pid.role()));
            std::stringstream ss;
            ss << _pid.range().from() << "_" << _pid.range().to();
            _remoteLogDir = FileSystem::joinFilePath(_remoteLogDir, ss.str());
            if (fslib::EC_OK != FileSystem::mkDir(_remoteLogDir, true)) {
                AUTIL_LOG(WARN, "mkdir [%s] failed, maybe created by others", _remoteLogDir.c_str());
            }
            AUTIL_LOG(INFO, "remote log dir [%s]", _remoteLogDir.c_str());
            return true;
        }
    }
    return false;
}

bool RemoteLogger::run(const std::string& bsWorkDir, const std::string& pipeFile, int64_t keepLogHour)
{
    if (keepLogHour == 0) {
        keepLogHour = 1;
    }
    // prepare self
    _startTime = autil::TimeUtility::currentTime();
    _pipeFile = pipeFile;
    _bsWorkDir = bsWorkDir;
    _pendingLogs.reset(new util::MemControlStreamQueue<std::string>(MAX_TOTAL_BUFFER_SIZE, MAX_TOTAL_BUFFER_SIZE,
                                                                    10000 /*wait time us*/));
    AUTIL_LOG(INFO, "start log uploader, pipe file [%s] bs work dir [%s]", _pipeFile.c_str(), _bsWorkDir.c_str());
    if (!initPid()) {
        AUTIL_LOG(ERROR, "init pid failed");
        return false;
    }

    // prepare local
    _sourceFd = openFifoFile(pipeFile);
    if (_sourceFd <= 0) {
        AUTIL_LOG(ERROR, "wait local file ready file failed, fd [%d]", _sourceFd);
        return false;
    }

    // prepare remote
    if (!calculateRemoteDir()) {
        AUTIL_LOG(ERROR, "calculate target root failed");
        return false;
    }
    _asyncWriteThread =
        autil::Thread::createThread(std::bind(&RemoteLogger::write, this, keepLogHour), "async_write_log");
    _asyncFlushThread =
        autil::LoopThread::createLoopThread(std::bind(&RemoteLogger::flush, this), 10 * 1000 * 1000, "async_flush_log");
    process();
    return true;
}

void RemoteLogger::write(int64_t keepLogHour)
{
    AUTIL_LOG(INFO, "begin write");
    bool eof = false;
    while (!eof) {
        std::string buf;
        uint32_t currentSize = _pendingLogs->size();
        std::string tmpBuf;
        for (uint32_t i = 0; i + 1 < currentSize; i++) {
            if (!_pendingLogs->pop(tmpBuf)) {
                eof = true;
                break;
            }
            buf += tmpBuf;
        }

        if (!_pendingLogs->pop(tmpBuf)) {
            eof = true;
        } else {
            buf += tmpBuf;
        }
        ssize_t writeSize = 0;

        {
            autil::ScopedLock lk(_mtx);
            while (!_outputFile) {
                _outputFile = genRemoteFile(keepLogHour);
            }
            writeSize = _outputFile->write(buf.data(), buf.length());
        }

        if (buf.length() != writeSize) {
            AUTIL_LOG(ERROR, "write log [%s] failed", buf.c_str());
            closeRemote();
        } else {
            _currentFileLength += buf.length();
            if (_currentFileLength > MAX_SINGLE_FILE_SIZE ||
                autil::TimeUtility::currentTimeInSeconds() - _lastFileCreateSecond >= keepLogHour * 3600 / 2) {
                closeRemote();
            }
        }
    }
}

void RemoteLogger::process()
{
    AUTIL_LOG(INFO, "begin process");
    while (true) {
        std::string buf(MAX_SINGLE_BUFFER_SIZE, '0');
        int32_t readCount = read(_sourceFd, buf.data(), MAX_SINGLE_BUFFER_SIZE);
        if (readCount <= 0) {
            if (_stopped) {
                AUTIL_LOG(INFO, "read finish, set finish to pendinglogs");
                _pendingLogs->setFinish();
                _asyncFlushThread.reset();
                _asyncWriteThread.reset();
                closeRemote();
                return;
            } else {
                AUTIL_LOG(INFO,
                          "read count is [%d] but still not receive stop flag, maybe major program failover sleep 1 "
                          "second ..",
                          readCount);
                usleep(1 * 1000 * 1000);
                continue;
            }
        }
        buf.resize(readCount);
        _pendingLogs->push(buf, readCount);
    }
}

void RemoteLogger::flush()
{
    autil::ScopedLock lk(_mtx);
    if (_outputFile) {
        if (fslib::EC_OK != _outputFile->flush()) {
            AUTIL_LOG(ERROR, " flush [%s] failed", _outputFile->getFileName());
        }
    }
}

void RemoteLogger::closeRemote()
{
    AUTIL_LOG(INFO, "close remote file");
    _currentFileLength = 0;
    autil::ScopedLock lk(_mtx);
    if (_outputFile) {
        if (fslib::EC_OK != _outputFile->flush()) {
            AUTIL_LOG(ERROR, " flush [%s] failed", _outputFile->getFileName());
        }
        if (fslib::EC_OK != _outputFile->close()) {
            AUTIL_LOG(ERROR, "close file [%s] failed", _outputFile->getFileName());
        }
        delete _outputFile;
        _outputFile = nullptr;
    }
}

void RemoteLogger::stop()
{
    AUTIL_LOG(INFO, "receive stop flag");
    _stopped = true;
}

void RemoteLogger::cleanExpiredLogs(const std::string& remoteLogDir, int64_t keepLogHour)
{
    fslib::RichFileList files;
    if (fslib::EC_OK != FileSystem::listDir(remoteLogDir, files)) {
        AUTIL_LOG(WARN, "list log dir [%s] failed", remoteLogDir.c_str());
        return;
    }
    for (const auto& file : files) {
        int64_t gapSecond = autil::TimeUtility::currentTimeInSeconds() - file.createTime;
        if (gapSecond >= keepLogHour * 3600) {
            AUTIL_LOG(INFO, "remote log file [%s], create second [%ld], keep log hour [%ld], gapSecond [%ld]",
                      file.path.c_str(), file.createTime, keepLogHour, gapSecond);
            [[maybe_unused]] auto ec = FileSystem::remove(FileSystem::joinFilePath(remoteLogDir, file.path));
        }
    }
}

}} // namespace build_service::tools
