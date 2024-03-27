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
#include <oss_c_sdk/aos_http_io.h>

#include "fslib/fs/oss/OssFileSystem.h"

extern "C" fslib::fs::AbstractFileSystem *createFileSystem() {
    static fslib::fs::oss::OssFileSystem fileSystem;
    if (aos_http_io_initialize(NULL, 0) != AOSE_OK) {
        return NULL;
    }
    return &fileSystem;
}

extern "C" void destroyFileSystem(fslib::fs::AbstractFileSystem *fileSystem) { aos_http_io_deinitialize(); }
