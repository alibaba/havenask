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
#include <sys/file.h>
#include <signal.h>
#include <errno.h>
#include "fslib/fs/local/LocalFileLock.h"
#include "fslib/util/LongIntervalLog.h"

using namespace std;
FSLIB_BEGIN_NAMESPACE(fs);
AUTIL_DECLARE_AND_SETUP_LOGGER(fs, LocalFileLock);

LocalFileLock::LocalFileLock(const string& fileName) { 
    _fd = open(fileName.c_str(), O_RDWR | O_CREAT, 0666);
    if (_fd == -1) {
        AUTIL_LOG(ERROR, "create file lock fail, %s.", strerror(errno));
    }
}

LocalFileLock::~LocalFileLock() { 
    if (_fd != -1) {
        close(_fd);
    }
}

void LocalFileLock::emptyHander(int signo) {
    return;
}

int LocalFileLock::lock_reg(int fd, int operation, uint32_t timeout)
{
    FSLIB_LONG_INTERVAL_LOG("fd[%d]", fd);
    if (_fd == -1) {
        AUTIL_LOG(ERROR, "operation fail, file description invalid.");
        return -1;
    }

    int sec = -1;
    struct sigaction act;
    struct sigaction oact;
    if (timeout != 0) {
        sigemptyset(&act.sa_mask);
        act.sa_handler = emptyHander;
        act.sa_flags = 0;
        sigaction(SIGALRM, &act, &oact);

        sec = alarm(timeout);
    }

    int ret = flock(fd, operation);
    if (ret == -1) {
        AUTIL_LOG(ERROR, "operation fail, %s.", strerror(errno));
    }

    if (timeout != 0) {
        alarm(sec);
        sigaction(SIGALRM, &oact, NULL);
    }    
    
    return ret;
}

int LocalFileLock::lock(uint32_t timeout) {
    FSLIB_LONG_INTERVAL_LOG(" ");
    return lock_reg(_fd, LOCK_EX, timeout);
}

int LocalFileLock::unlock() {
    FSLIB_LONG_INTERVAL_LOG(" ");
    return lock_reg(_fd, LOCK_UN, 0);
}

FSLIB_END_NAMESPACE(fs);

