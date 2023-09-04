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
#pragma once
#include "autil/Lock.h"
#include "autil/LoopThread.h"
#include "autil/Thread.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "build_service/util/MemControlStreamQueue.h"
namespace fslib { namespace fs {
class File;
}} // namespace fslib::fs
namespace build_service { namespace tools {

class RemoteLogger
{
public:
    RemoteLogger();
    ~RemoteLogger();

    static const uint32_t MAX_TOTAL_BUFFER_SIZE = 100 * 1024 * 1024; // 100M
    static const uint32_t MAX_SINGLE_BUFFER_SIZE = 4096;             // 4k
    static const uint32_t MAX_SINGLE_FILE_SIZE = 100 * 1024 * 1024;  // 100M

public:
    bool run(const std::string& bsWorkDir, const std::string& pipeFile, int64_t keepLogHour);
    void stop();

private:
    bool initPid();
    bool calculateRemoteDir();
    void closeRemote();
    int32_t openFifoFile(const std::string& fileName);
    void processLogs();
    fslib::fs::File* genRemoteFile(int64_t keepLogHour);
    void write(int64_t keepLogHour);
    void flush();
    void process();
    static void cleanExpiredLogs(const std::string& remoteLogDir, int64_t keepLogHour);

    std::string extractRoleFromPath(const std::string& path);

private:
    // remote
    fslib::fs::File* _outputFile;
    autil::ThreadMutex _mtx;
    autil::ThreadPtr _asyncWriteThread;
    autil::LoopThreadPtr _asyncFlushThread;
    std::string _remoteLogDir;
    int32_t _remoteFileIdx;
    size_t _currentFileLength;

    // local
    std::string _bsWorkDir;
    std::string _pipeFile;
    int32_t _sourceFd;

    // self
    std::shared_ptr<util::MemControlStreamQueue<std::string>> _pendingLogs;
    std::atomic<bool> _stopped;
    proto::PartitionId _pid;
    int64_t _startTime;
    int64_t _lastCleanExpiredLogSecond;
    int64_t _lastFileCreateSecond;

private:
    std::string _TEST_cwd;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace build_service::tools
