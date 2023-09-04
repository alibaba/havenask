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

#include <stdint.h>

#include "indexlib/file_system/FileSystemDefine.h"

namespace indexlib { namespace file_system {

//当mount路径与已有路径冲突时的处理方式
enum class ConflictResolution {
    SKIP,       // 跳过同名文件
    OVERWRITE,  // 覆盖同名文件
    CHECK_DIFF, // 同名但不同物理路径则报错
    // SYNC,  // 镜像级别的同步，会删除 target 上多余的文件
};

struct MountOption {
    FSMountType mountType = FSMT_READ_ONLY;
    ConflictResolution conflictResolution = ConflictResolution::OVERWRITE;

    MountOption(FSMountType type) : mountType(type), conflictResolution(ConflictResolution::OVERWRITE) {}
};

}} // namespace indexlib::file_system
