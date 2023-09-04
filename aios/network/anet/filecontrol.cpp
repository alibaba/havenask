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
#include "aios/network/anet/filecontrol.h"

#include <fcntl.h>
namespace anet {
bool FileControl::setCloseOnExec(int fd) {
    int flags;
    flags = fcntl(fd, F_GETFD);
    if (flags == -1) {
        return false;
    }
    flags |= FD_CLOEXEC;
    return fcntl(fd, F_SETFD, flags) != -1;
}

bool FileControl::clearCloseOnExec(int fd) {
    int flags;
    flags = fcntl(fd, F_GETFD);
    if (flags == -1) {
        return false;
    }
    flags &= ~FD_CLOEXEC;
    return fcntl(fd, F_SETFD, flags) != -1;
}

bool FileControl::isCloseOnExec(int fd) {
    int flags;
    flags = fcntl(fd, F_GETFD);
    if (flags == -1) {
        return false;
    }
    return flags & FD_CLOEXEC;
}

} // namespace anet
