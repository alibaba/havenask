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

#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "fslib/common/common_type.h"

namespace indexlib { namespace file_system {

static constexpr const char* TEMP_FILE_SUFFIX = ".__tmp__";
static constexpr const char* PACKAGE_FILE_PREFIX = "package_file";
static constexpr const char* PACKAGE_FILE_DATA_SUFFIX = ".__data__";
static constexpr const char* PACKAGE_FILE_META_SUFFIX = ".__meta__";
static constexpr const char* PACKAGE_FILE_META_SUFFIX_WITHOUT_DOT = "__meta__";
static constexpr const char* ENTRY_TABLE_FILE_NAME_PREFIX = "entry_table";
static constexpr const char* ENTRY_TABLE_FILE_NAME_DOT_PREFIX = "entry_table.";
static constexpr const char* ENTRY_TABLE_PRELOAD_FILE_NAME = "entry_table.preload";
static constexpr const char* ENTRY_TABLE_PRELOAD_BACK_UP_FILE_NAME = "entry_table.preload.back";
static constexpr const char* FILE_SYSTEM_PATCH_DOT_PREFIX = "patch.";
static constexpr const char* FILE_SYSTEM_INNER_SUFFIX = ".__fs__";
static constexpr const char* COMPRESS_HINT_SAMPLE_RATIO = "hint_sample_ratio";
static constexpr const char* COMPRESS_HINT_SAMPLE_BLOCK_COUNT = "hint_sample_block_count";
static constexpr const char* COMPRESS_ENABLE_HINT_PARAM_NAME = "enable_hint_data";
static constexpr const char* COMPRESS_HINT_DATA_CAPACITY = "hint_data_capacity";
static constexpr const char* COMPRESS_HINT_ALWAYS_ADAPTIVE = "hint_always_adaptive";
static constexpr const char* COMPRESS_ENABLE_META_FILE = "enable_meta_file";
static constexpr const char* COMPRESS_FILE_INFO_SUFFIX = ".__compress_info__";
static constexpr const char* COMPRESS_FILE_META_SUFFIX = ".__compress_meta__";
static constexpr const char* FILE_SYSTEM_ROOT_LINK_NAME = "__indexlib_fs_root_link__";
static constexpr const char* DISABLE_FIELDS_LOAD_CONFIG_NAME = "__DISABLE_FIELDS__";
static constexpr const char* DEPLOY_META_LOCK_SUFFIX = "._ilock_";

inline const std::string SEGMENT_PATTERN = "^(__indexlib_fs_root_link__(@[0-9]+)?/(__FENCE__)?rt_index_partition/"
                                           ")?(patch_index_[0-9]+/)?segment_[0-9]+(_level_[0-9]+)?";
inline const std::string SEGMENT_SUB_PATTERN = SEGMENT_PATTERN + "(/sub_segment)?";
inline const std::string SEGMENT_KV_PATTERN = SEGMENT_PATTERN + "(/shard_[0-9]+)?";

enum FSStorageType {
    FSST_MEM = 0,
    FSST_DISK,
    FSST_PACKAGE_MEM,
    FSST_PACKAGE_DISK,
    FSST_UNKNOWN,
};

enum FSOpenType {
    FSOT_MEM = 0,
    FSOT_MMAP,  // mmap private read+write
    FSOT_CACHE, // block cache
    FSOT_BUFFERED,
    FSOT_SLICE,
    FSOT_RESOURCE,
    // synthesize open type
    FSOT_LOAD_CONFIG, // by load config list
    FSOT_MEM_ACCESS,  // original name is Integrated, need support getBaseAddress()
    FSOT_UNKNOWN,
};

enum FSFileType {
    FSFT_MEM = 0,
    FSFT_MMAP,
    FSFT_MMAP_LOCK,
    FSFT_BLOCK, // block cache
    FSFT_BUFFERED,
    FSFT_SLICE,
    FSFT_DIRECTORY,
    FSFT_RESOURCE,
    FSFT_UNKNOWN, // must be the last one for StorageMetrics
};

enum FSMetricGroup {
    FSMG_LOCAL,
    FSMG_MEM,
    FSMG_UNKNOWN,
};

inline const char* GetFileNodeTypeStr(FSFileType type)
{
    switch (type) {
    case FSFT_MEM:
        return "mem";
    case FSFT_MMAP:
        return "mmap";
    case FSFT_MMAP_LOCK:
        return "mmap_lock";
    case FSFT_BLOCK:
        return "block";
    case FSFT_BUFFERED:
        return "buffered";
    case FSFT_SLICE:
        return "slice";
    case FSFT_DIRECTORY:
        return "directory";
    case FSFT_RESOURCE:
        return "resource";
    case FSFT_UNKNOWN:
        return "unknown";
    default:
        return "default";
    }
    return "none";
}

enum FSMetricPreference {
    FSMP_ALL = 0,
    FSMP_OFFLINE = 1,
    FSMP_ONLINE = FSMP_ALL,
};

enum FSMountType { FSMT_READ_ONLY = 0, FSMT_READ_WRITE };

struct FileStat {
    /* FSStorageType storageType; */
    FSFileType fileType = FSFT_UNKNOWN; // valid when inCache == true
    FSOpenType openType = FSOT_UNKNOWN; // valid when inCache == true
    size_t fileLength = 0;
    bool inCache = false;
    /* bool onDisk; */
    bool inPackage;
    bool isDirectory = false;
    int64_t offset;
    std::string physicalRoot;
    std::string physicalPath;
};

struct SingleIO {
    void* buffer;
    size_t len;
    size_t offset;
    SingleIO() = default;
    SingleIO(void* buffer, size_t len, size_t offset)
    {
        this->buffer = buffer;
        this->len = len;
        this->offset = offset;
    }
    bool operator<(const SingleIO& rhs) const { return offset < rhs.offset; }
};
typedef std::vector<SingleIO> BatchIO;

using FileList = fslib::FileList;
typedef int32_t segmentid_t;
typedef int32_t versionid_t;
const versionid_t INVALID_VERSIONID = -1;
const bool DEFAULT_COMPRESS_ENABLE_META_FILE = false;

}} // namespace indexlib::file_system
