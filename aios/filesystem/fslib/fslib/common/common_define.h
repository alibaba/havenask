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
#ifndef FSLIB_COMMON_DEFINE_H_
#define FSLIB_COMMON_DEFINE_H_

#include <cstdlib>
#include <memory>
#define FSLIB_BEGIN_NAMESPACE(x)                                                                                       \
    namespace fslib {                                                                                                  \
    namespace x {
#define FSLIB_END_NAMESPACE(x)                                                                                         \
    }                                                                                                                  \
    }
#define FSLIB_USE_NAMESPACE(x) using namespace fslib::x

#define BEGIN_FSLIB_NAMESPACE namespace fslib {
#define END_FSLIB_NAMESPACE }

#define FSLIB_NS(x) fslib::x

#define FSLIB_TYPEDEF_AUTO_PTR(x) typedef std::unique_ptr<x> x##Ptr
#define FSLIB_TYPEDEF_SHARED_PTR(x) typedef std::shared_ptr<x> x##Ptr

#define FSLIB_EC_OK_ERRSTR "operation succeed!"
#define FSLIB_EC_BADF_ERRSTR "bad file descriptor!"
#define FSLIB_EC_BADARGS_ERRSTR "bad arguments!"
#define FSLIB_EC_BUSY_ERRSTR "device or resource busy!"
#define FSLIB_EC_CONNECTIONLOSS_ERRSTR "connection is lost!"
#define FSLIB_EC_EXIST_ERRSTR "path exist!"
#define FSLIB_EC_ISDIR_ERRSTR "path is a directory!"
#define FSLIB_EC_KUAFU_ERRSTR "fail to resolve nuwa address!"
#define FSLIB_EC_NOENT_ERRSTR "path does not exist!"
#define FSLIB_EC_NOTDIR_ERRSTR "path is not a directory!"
#define FSLIB_EC_NOTEMPTY_ERRSTR "directory is not empty!"
#define FSLIB_EC_NOTSUP_ERRSTR "action not support!"
#define FSLIB_EC_OPERATIONTIMEOUT_ERRSTR "operation timeout!"
#define FSLIB_EC_PARSEFAIL_ERRSTR "parse file protocol fail!"
#define FSLIB_EC_UNKNOWN_ERRSTR "unknown error!"
#define FSLIB_EC_XDEV_ERRSTR "cross-device link!"
#define FSLIB_EC_PERMISSION_ERRSTR "no permission!"
#define FSLIB_EC_INVALIDSTATE_ERRSTR "invalid state!"
#define FSLIB_EC_INIT_ZOOKEEPER_FAIL_ERRSTR "init zookeeper handler fail!"
#define FSLIB_EC_AGAIN_ERRSTR "resource temporarily unavailable!"
#define FSLIB_EC_SESSIONEXPIRED_ERRSTR "session expired!"
#define FSLIB_EC_PANGU_FILE_LOCK_ERRSTR "pangu file opened by others!"
#define FSLIB_EC_LOCAL_DISK_NO_SPACE "local disk no space!"

#define FSLIB_ERROR_CODE_NOT_SUPPORT "get error string fail, unknown error code!"

#define FSLIB_REMOVE_COLLECTOR_NAME "fslib_remove"
#define FSLIB_ERROR_COLLECTOR_NAME "fslib_error"
#define FSLIB_LONG_INTERVAL_COLLECTOR_NAME "fslib_long_interval"

#define FSLIB_MAX_INT ((1 << 31) - 1)
#define DEFAULT_FILE_BLOCK_SIZE 1024 * 1024
#define DEFAULT_MEM_POOL_SIZE 1024 * 1024 * 1024
#define MAX_UNWRITTEN_LEN (int64_t)10 * 1024 * 1024 * 1024

#define FSLIB_FS_NUM 5

#define FSLIB_FS_UNKNOWN_FILESYSTEM "UNKNOWN"
#define FSLIB_FS_LOCAL_FILESYSTEM_NAME "LOCAL"
#define FSLIB_FS_ZFS_FILESYSTEM_NAME "zfs"
#define FSLIB_FS_HTTP_FILESYSTEM_NAME "http"
#define FSLIB_FS_DFS_FILESYSTEM_NAME "dfs"
#define FSLIB_FS_HDFS_FILESYSTEM_NAME "hdfs"
#define FSLIB_FS_PANGU_FILESYSTEM_NAME "pangu"
#define FSLIB_FS_OSS_FILESYSTEM_NAME "oss"

#define FSLIB_FS_LIBSO_PATH_ENV_NAME "LD_LIBRARY_PATH"
#define FSLIB_FS_LIBSO_NAME_PREFIX "libfslib_plugin_"
#define FSLIB_FS_LIBSO_NAME_PATTERN "libfslib_plugin_[xxx].so.[1]"
#define FSLIB_FS_DLL_CREATE_FUNCTION_NAME "createFileSystem"
#define FSLIB_FS_DLL_DESTROY_FUNCTION_NAME "destroyFileSystem"
#define FSLIB_FS_LIBSO_MAX_VERSION 0xFFFFFFFF

#define FSLIB_LISTDIR_REUSE_THREADPOOL "FSLIB_LISTDIR_REUSE_THREADPOOL"
#define FSLIB_LISTDIR_THREADNUM "FSLIB_LISTDIR_THREADNUM"
#define FSLIB_CREATE_FS_RETRY_INTERVAL "FSLIB_CREATE_FS_RETRY_INTERVAL"
#define FSLIB_COPY_BLOCK_SIZE "FSLIB_COPY_BLOCK_SIZE"
#define FSLIB_COPY_IS_OVERRIDE "FSLIB_COPY_IS_OVERRIDE"
#define FSLIB_META_CHECK_CACHE "FSLIB_META_CHECK_CACHE"

#define FSLIB_ERROR_CONFIG_PATH "FSLIB_ERROR_CONFIG_PATH"
#define OPERATION_OPEN_FILE "openFile"
#define OPERATION_MMAP_FILE "mmapFile"
#define OPERATION_RENAME "rename"
#define OPERATION_GET_FILE_META "getFileMeta"
#define OPERATION_ISFILE "isFile"
#define OPERATION_COPY "copy"
#define OPERATION_LINK "link"
#define OPERATION_MOVE "move"
#define OPERATION_MKDIR "mkdir"
#define OPERATION_LISTDIR "listDir"
#define OPERATION_ISDIR "isDirectory"
#define OPERATION_REMOVE "remove"
#define OPERATION_ISEXIST "isExist"
#define OPERATION_READ "read"
#define OPERATION_WRITE "write"
#define OPERATION_PREAD "pread"
#define OPERATION_PWRITE "pwrite"
#define OPERATION_FLUSH "flush"
#define OPERATION_CLOSE "close"
#define OPERATION_SEEK "seek"
#define OPERATION_TELL "tell"
#define OPERATION_FORWARD "forward"

#define ERROR_TYPE_METHOD "method"
#define ERROR_TYPE_ERROR_CODE "errorcode"
#define ERROR_TYPE_OFFSET "offset"
#define ERROR_TYPE_DELAY "delay"
#define ERROR_TYPE_RETRY "retry"

//---------- fslib metric
#define FSLIB_ENABLE_REPORT_METRIC "FSLIB_ENABLE_REPORT_METRIC"
#define FSLIB_KMONITOR_SERVICE_NAME "FSLIB_KMON_SERVICE_NAME"
#define FSLIB_METRIC_DEFAULT_SERVICE_NAME "storage"
#define FSLIB_METRIC_DFS_QPS "meta.qps"
#define FSLIB_METRIC_DFS_ERROR_QPS "meta.errorQps"
#define FSLIB_METRIC_DFS_LATENCY "meta.latency"

#define FSLIB_METRIC_DN_ERROR_QPS "data.errorQps"
#define FSLIB_METRIC_DN_READ_ERROR_QPS "data.read.errorQps"
#define FSLIB_METRIC_DN_READ_SPEED "data.read.speed"
#define FSLIB_METRIC_DN_READ_LATENCY "data.read.latency"
#define FSLIB_METRIC_DN_READ_AVG_LATENCY "data.read.latencyPerUnit"
#define FSLIB_METRIC_DN_WRITE_ERROR_QPS "data.write.errorQps"
#define FSLIB_METRIC_DN_WRITE_SPEED "data.write.speed"
#define FSLIB_METRIC_DN_WRITE_LATENCY "data.write.latency"
#define FSLIB_METRIC_DN_WRITE_AVG_LATENCY "data.write.latencyPerUnit"

#define FSLIB_METRIC_META_CACHE_PATH_COUNT "cache.meta.pathCount"
#define FSLIB_METRIC_META_CACHE_IMMUTABLE_PATH_COUNT "cache.meta.immutablePathCount"
#define FSLIB_METRIC_META_CACHE_HIT_QPS "cache.meta.hitQps"

#define FSLIB_METRIC_DATA_CACHE_MEM_USE "cache.data.memoryUse"
#define FSLIB_METRIC_DATA_CACHE_FILE_COUNT "cache.data.fileCount"
#define FSLIB_METRIC_DATA_CACHE_HIT_QPS "cache.data.hitQps"

//---------- metric tag & value
#define FSLIB_METRIC_TAGS_DFS_TYPE "dfsType"
#define FSLIB_METRIC_TAGS_DFS_NAME "dfsName"
#define FSLIB_METRIC_TAGS_OPTYPE "opType"
#define FSLIB_METRIC_TAGS_OPTYPE_OPEN4READ "open4read"
#define FSLIB_METRIC_TAGS_OPTYPE_OPEN4WRITE "open4write"
#define FSLIB_METRIC_TAGS_OPTYPE_OPEN4APPEND "open4append"
#define FSLIB_METRIC_TAGS_OPTYPE_RENAME "rename"
#define FSLIB_METRIC_TAGS_OPTYPE_GETFILEMETA                                                                           \
    "getfilemeta" // getFileMeta, getPathMeta, isFile, isDir, isExist are the same operation
#define FSLIB_METRIC_TAGS_OPTYPE_LIST "list"
#define FSLIB_METRIC_TAGS_OPTYPE_MKDIR "mkdir"
#define FSLIB_METRIC_TAGS_OPTYPE_DELETE "delete"
#define FSLIB_METRIC_TAGS_OPTYPE_DELETE_FILE "deletefile"
#define FSLIB_METRIC_TAGS_OPTYPE_DELETE_DIR "deletedir"
#define FSLIB_METRIC_TAGS_OPTYPE_CREATE_FILELOCK "createfilelock"     // only works for zookeeper
#define FSLIB_METRIC_TAGS_OPTYPE_CREATE_READWRITE_LOCK "createRWlock" // only works for local fs

#define FSLIB_METRIC_TAGS_OPTYPE_FLUSH "flush"
#define FSLIB_METRIC_TAGS_OPTYPE_SYNC "sync"
#define FSLIB_METRIC_TAGS_OPTYPE_CLOSE4READ "close4read"
#define FSLIB_METRIC_TAGS_OPTYPE_CLOSE4WRITE "close4write"
#define FSLIB_METRIC_TAGS_OPTYPE_SEEK "seek"

#endif /*FSLIB_COMMON_DEFINE_H_*/
