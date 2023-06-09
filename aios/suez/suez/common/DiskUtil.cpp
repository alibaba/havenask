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
#include "suez/common/DiskUtil.h"

#include <ftw.h>
#include <sys/stat.h>
#include <sys/statfs.h>

namespace suez {

int64_t DiskUtil::totalDirectorySize = 0;

int DiskUtil::sumDirectory(const char *fpath, const struct stat *sb, int typeflag) {
    DiskUtil::totalDirectorySize += sb->st_size;
    return 0;
}

bool DiskUtil::GetDirectorySize(const char *dir, int64_t &size) {
    DiskUtil::totalDirectorySize = 0;
    if (ftw(dir, &sumDirectory, 1)) {
        return false;
    }
    size = DiskUtil::totalDirectorySize;
    return true;
}

bool DiskUtil::GetDiskInfo(int64_t &usedDiskSize, int64_t &totalDiskSize) {
    struct statfs diskInfo;
    if (0 != statfs("./", &diskInfo)) {
        return false;
    }
    int64_t blockSize = diskInfo.f_bsize;
    totalDiskSize = blockSize * diskInfo.f_blocks;
    usedDiskSize = totalDiskSize - diskInfo.f_bavail * blockSize;
    return true;
}

} // namespace suez
