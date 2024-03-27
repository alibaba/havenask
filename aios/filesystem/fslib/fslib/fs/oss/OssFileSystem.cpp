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
#include "fslib/fs/oss/OssFileSystem.h"

#include <autil/StringUtil.h>

#include "autil/EnvUtil.h"
#include "fslib/fs/oss/OssFile.h"
#include "fslib/fs/oss/havenask/OssConfig.h"

using namespace std;
using namespace autil;

FSLIB_PLUGIN_BEGIN_NAMESPACE(oss);
AUTIL_DECLARE_AND_SETUP_LOGGER(oss, OssFileSystem);

const string OssFileSystem::CONFIG_SEPARATOR("?");
const string OssFileSystem::KV_SEPARATOR("=");
const string OssFileSystem::KV_PAIR_SEPARATOR("&");
const string OssFileSystem::OSS_PREFIX("oss://");

const string OssFileSystem::ENV_DEFAULT_OSS_ACCESS_ID("HIPPO_DEFAULT_OSS_ACCESS_ID");
const string OssFileSystem::ENV_DEFAULT_OSS_ACCESS_KEY("HIPPO_DEFAULT_OSS_ACCESS_KEY");
const string OssFileSystem::ENV_DEFAULT_OSS_ENDPOINT("HIPPO_DEFAULT_OSS_ENDPOINT");

const string OssFileSystem::ENV_OSS_CONFIG_PATH("HIPPO_OSS_CONFIG_PATH");

const string OssFileSystem::OSS_ACCESS_ID("OSS_ACCESS_ID");
const string OssFileSystem::OSS_ACCESS_KEY("OSS_ACCESS_KEY");
const string OssFileSystem::OSS_ENDPOINT("OSS_ENDPOINT");

const string OssFileSystem::DEFAULT_OSS_CONFIG_PATH("/apsara/.osscredentials");

File *OssFileSystem::openFile(const string &fileName, Flag flag) {
    string bucket;
    string object;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    if (!parsePath(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint)) {
        return new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_BADARGS);
    }

    flag = (Flag)(flag & FLAG_CMD_MASK);

    switch (flag) {
    case READ: {
        OssFile *ossFile = new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_OK);
        if (ossFile->initFileInfo() != EC_OK) {
            ossFile->close();
        }

        return ossFile;
    }
    case WRITE: {
        if (isExist(fileName) == EC_TRUE) {
            // delete & ignore return, same with pangu plugin
            remove(fileName);
        }
        OssFile *ossFile = new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_OK);
        // create file
        const char *emptyStr = "";
        ossFile->write(emptyStr, 0);
        return ossFile;
    }
    case APPEND: {
        OssFile *ossFile = new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_OK);
        if (isExist(fileName) == EC_TRUE) {
            if (!ossFile->updateAppendPos()) {
                // file is not appendable
                delete ossFile;
                return new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_BADF);
            }
        } else {
            // OSS规定：必须以append接口创建的文件，才能追加写入
            // (https://help.aliyun.com/document_detail/31981.html)
            const char *emptyStr = "";
            ossFile->write(emptyStr, 0);
        }
        return ossFile;
    }
    default: {
        return new OssFile(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, flag, EC_NOTSUP);
    }
    }
}

bool OssFileSystem::getOssConfigFromEnv(string &accessKeyId, string &accessKeySecret, string &endpoint) {
    string accessKeyIdStr = autil::EnvUtil::getEnv(ENV_DEFAULT_OSS_ACCESS_ID);
    if (accessKeyIdStr.empty()) {
        return false;
    }
    accessKeyId = accessKeyIdStr;
    string accessKeySecretStr = autil::EnvUtil::getEnv(ENV_DEFAULT_OSS_ACCESS_KEY);
    if (accessKeySecretStr.empty()) {
        return false;
    }
    accessKeySecret = accessKeySecretStr;

    string endpointStr = autil::EnvUtil::getEnv(ENV_DEFAULT_OSS_ENDPOINT);
    if (endpointStr.empty()) {
        return false;
    }
    endpoint = endpointStr;
    return true;
}

bool OssFileSystem::parseBucketAndObject(const string &fullPathWithOutConfigs, string &bucket, string &object) {
    vector<string> items = StringUtil::split(fullPathWithOutConfigs, OSS_PREFIX);
    if (items.size() < 1) {
        AUTIL_LOG(ERROR, "parse bucket and object failed, invalid path:[%s]", fullPathWithOutConfigs.c_str());
        return false;
    }
    vector<string> splitedPaths = StringUtil::split(items[0], "/");
    bucket = splitedPaths[0];
    object = "";
    for (size_t i = 1; i < splitedPaths.size(); i++) {
        if (object != "") {
            object += "/";
        }
        object += splitedPaths[i];
    }
    if (bucket == "" || object == "") {
        return false;
    }
    return true;
}

bool OssFileSystem::parseOssConfig(const string &configs,
                                   string &accessKeyId,
                                   string &accessKeySecret,
                                   string &endpoint) {
    vector<string> kvs = StringUtil::split(configs, KV_PAIR_SEPARATOR);
    accessKeyId = "";
    accessKeySecret = "";
    endpoint = "";
    for (size_t i = 0; i < kvs.size(); i++) {
        vector<string> items = StringUtil::split(kvs[i], KV_SEPARATOR);
        if (items.size() >= 2) {
            if (items[0] == OSS_ACCESS_ID) {
                accessKeyId = items[1];
            }
            if (items[0] == OSS_ACCESS_KEY) {
                accessKeySecret = items[1];
            }
            if (items[0] == OSS_ENDPOINT) {
                endpoint = items[1];
            }
        }
    }
    if (accessKeyId == "" || accessKeySecret == "" || endpoint == "") {
        return false;
    }
    return true;
}

bool OssFileSystem::parseConfigStr(const string &fullPath, string &configs, string &path) {
    vector<string> items = StringUtil::split(fullPath, CONFIG_SEPARATOR);
    if (items.empty()) {
        return false;
    } else if (items.size() == 1) {
        path = fullPath;
        configs = "";
    }
    // support oss://aa/bb?arg1=1&arg2=2/cc and oss://aa/bb?arg1=1&arg2=2
    else if (items.size() == 2) {
        path = items[0];
        string::size_type path_seporator_pos = items[1].find("/");
        configs = items[1].substr(0, path_seporator_pos);
        if (path_seporator_pos != string::npos) {
            // avoid to oss://aa/bb/?arg1=1&arg2=2/cc oss://aa/bb//cc
            if (path[path.size() - 1] == '/') {
                if (path_seporator_pos + 1 < items[1].size()) {
                    path += items[1].substr(path_seporator_pos + 1);
                }
            } else {
                path += items[1].substr(path_seporator_pos);
            }
        }
    } else { // don't support oss://aa/bb?arg1=1/cc?arg2=2
        AUTIL_LOG(ERROR, "don't support oss path: %s", fullPath.c_str());
        return false;
    }

    return true;
}

bool OssFileSystem::parseOssConfigFromFile(string &accessKeyId, string &accessKeySecret, string &endpoint) {
    string configFilePath = autil::EnvUtil::getEnv(ENV_OSS_CONFIG_PATH);
    if (configFilePath.empty()) {
        configFilePath = DEFAULT_OSS_CONFIG_PATH.c_str();
    }
    OssConfig ossConfig(configFilePath);
    if (!ossConfig.readConfig()) {
        return false;
    }
    accessKeyId = ossConfig.getOssAccessId();
    accessKeySecret = ossConfig.getOssAccessKey();
    endpoint = ossConfig.getOssEndpoint();
    return true;
}

bool OssFileSystem::parsePath(const string &fullPath,
                              string &bucket,
                              string &object,
                              string &accessKeyId,
                              string &accessKeySecret,
                              string &endpoint) {
    string configs, path;
    parseConfigStr(fullPath, configs, path);
    if (!parseBucketAndObject(path, bucket, object)) {
        AUTIL_LOG(ERROR, "get access bucket and object from path failed.");
        return false;
    }
    if (configs == "") {
        if (getOssConfigFromEnv(accessKeyId, accessKeySecret, endpoint)) {
            return true;
        } else if (parseOssConfigFromFile(accessKeyId, accessKeySecret, endpoint)) {
            return true;
        } else {
            AUTIL_LOG(ERROR, "get access id and key from system failed.");
            return false;
        }
    }
    if (parseOssConfig(configs, accessKeyId, accessKeySecret, endpoint)) {
        return true;
    }
    AUTIL_LOG(ERROR, "get access id and key from path failed.");
    return false;
}

MMapFile *OssFileSystem::mmapFile(
    const string &fileName, Flag openMode, char *start, size_t length, int prot, int mapFlag, off_t offset) {
    return new MMapFile(fileName, -1, NULL, -1, -1, EC_NOTSUP);
}

ErrorCode OssFileSystem::rename(const string &oldPath, const string &newPath) {

    // oss无rename能力，需要通过copy，delete实现
    // https://help.aliyun.com/knowledge_detail/65423.html
    // 检查newPath不存在
    ErrorCode ret = isDirectory(newPath);
    if (ret == EC_TRUE) {
        AUTIL_LOG(INFO, "new path is directory already exist");
        return EC_EXIST;
    }
    ret = isFile(newPath);
    if (ret == EC_TRUE) {
        AUTIL_LOG(INFO, "new path is file already exist");
        return EC_EXIST;
    }
    // 检查oldPath存在
    bool sourceObjectIsFile = false;
    ErrorCode isFileRet = isFile(oldPath);
    ErrorCode isDirectoryRet = isDirectory(oldPath);
    if (isFileRet != EC_TRUE && isDirectoryRet != EC_TRUE) {
        AUTIL_LOG(ERROR, "old path not exist");
        return EC_NOENT;
    }
    if (newPath == oldPath) {
        return EC_OK;
    }

    string sourceBucket;
    string sourceObject;
    string sourceAccessKeyId;
    string sourceAccessKeySecret;
    string sourceEndpoint;

    if (!parsePath(oldPath, sourceBucket, sourceObject, sourceAccessKeyId, sourceAccessKeySecret, sourceEndpoint)) {
        return EC_BADARGS;
    }
    string dstBucket;
    string dstObject;
    string dstAccessKeyId;
    string dstAccessKeySecret;
    string dstEndpoint;
    if (!parsePath(newPath, dstBucket, dstObject, dstAccessKeyId, dstAccessKeySecret, dstEndpoint)) {
        return EC_BADARGS;
    }
    if (sourceAccessKeyId != dstAccessKeyId || sourceAccessKeySecret != dstAccessKeySecret ||
        sourceEndpoint != dstEndpoint) {
        // 不支持不同key之间的操作
        AUTIL_LOG(ERROR, "the keyid,key secret, endpoint of source and dst not equal");
        return EC_BADARGS;
    }

    if (isFileRet == EC_TRUE) {
        sourceObjectIsFile = true;
    }
    if (isDirectoryRet == EC_TRUE) {
        sourceObject = normalizePath(sourceObject);
        dstObject = normalizePath(dstObject);
    }

    ret = renameObjectRecursive(sourceAccessKeyId,
                                sourceAccessKeySecret,
                                sourceEndpoint,
                                sourceBucket,
                                dstBucket,
                                sourceObject,
                                dstObject,
                                sourceObjectIsFile);
    return ret;
}

ErrorCode OssFileSystem::link(const string &oldPath, const string &newPath) {
    AUTIL_LOG(ERROR, "link [%s] to [%s] fail, oss does not support link", oldPath.c_str(), newPath.c_str());
    return EC_NOTSUP;
}

ErrorCode OssFileSystem::renameObjectRecursive(const string &accessKeyId,
                                               const string &accessKeySecret,
                                               const string &endpoint,
                                               const string &sourceBucketStr,
                                               const string &dstBucketStr,
                                               const string &sourceObjectStr,
                                               const string &dstObjectStr,
                                               bool objectIsFile) {
    ErrorCode ret;
    bool hasSubFile = false;
    // 文件先拷贝，然后返回
    if (objectIsFile) {
        ret = renameObject(accessKeyId,
                           accessKeySecret,
                           endpoint,
                           sourceBucketStr,
                           dstBucketStr,
                           sourceObjectStr,
                           dstObjectStr,
                           false);
        return ret;
    }
    vector<string> files;
    vector<string> dirs;

    ret = listObjects(accessKeyId, accessKeySecret, endpoint, sourceBucketStr, sourceObjectStr, "/", 1000, files, dirs);
    if (ret != EC_OK) {
        return ret;
    }

    for (auto &&f : files) {
        string sourceObjectTempStr = sourceObjectStr + f;
        string dstObjectTempStr = dstObjectStr + f;
        ret = renameObject(accessKeyId,
                           accessKeySecret,
                           endpoint,
                           sourceBucketStr,
                           dstBucketStr,
                           sourceObjectTempStr,
                           dstObjectTempStr,
                           false);
        if (ret != EC_OK) {
            return ret;
        }
        hasSubFile = true;
    }

    for (auto &&d : dirs) {
        string sourceObjectTempStr = normalizePath(normalizePath(sourceObjectStr) + d);
        string dstObjectTempStr = normalizePath(normalizePath(dstObjectStr) + d);
        hasSubFile = true;
        ret = renameObjectRecursive(accessKeyId,
                                    accessKeySecret,
                                    endpoint,
                                    sourceBucketStr,
                                    dstBucketStr,
                                    sourceObjectTempStr,
                                    dstObjectTempStr,
                                    false);
        if (ret != EC_OK) {
            return ret;
        }
    }
    // 目录先拷贝
    ret = renameObject(accessKeyId,
                       accessKeySecret,
                       endpoint,
                       sourceBucketStr,
                       dstBucketStr,
                       sourceObjectStr,
                       dstObjectStr,
                       hasSubFile);
    return ret;
}

ErrorCode OssFileSystem::copyObject(const string &accessKeyId,
                                    const string &accessKeySecret,
                                    const string &endpoint,
                                    const string &sourceBucketStr,
                                    const string &dstBucketStr,
                                    const string &sourceObjectStr,
                                    const string &dstObjectStr) {
    aos_pool_t *p = NULL;
    aos_string_t sourceBucket;
    aos_string_t dstBucket;
    aos_string_t sourceObject;
    aos_string_t dstObject;
    oss_request_options_t *options = NULL;
    int is_cname = 0;
    aos_status_t *s = NULL;
    aos_list_t buffer;
    aos_table_t *headers = NULL;
    aos_table_t *resp_headers = NULL;
    ErrorCode ret;
    // init pool
    aos_pool_create(&p, NULL);
    options = oss_request_options_create(p);

    // init config
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->endpoint, endpoint.c_str());
    aos_str_set(&options->config->access_key_id, accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, accessKeySecret.c_str());
    options->config->is_cname = is_cname;
    options->ctl = aos_http_controller_create(options->pool, 0);
    options->ctl->options->connect_timeout = 3600 * 1000 * 100;
    aos_str_set(&sourceBucket, sourceBucketStr.c_str());
    aos_str_set(&dstBucket, dstBucketStr.c_str());
    aos_str_set(&sourceObject, sourceObjectStr.c_str());
    aos_str_set(&dstObject, dstObjectStr.c_str());

    headers = aos_table_make(options->pool, 0);

    aos_list_init(&buffer);

    s = oss_copy_object(options, &sourceBucket, &sourceObject, &dstBucket, &dstObject, headers, &resp_headers);
    if (aos_status_is_ok(s)) {
        ret = EC_OK;
    } else {
        AUTIL_LOG(ERROR,
                  "oss copy %s to %s  failed, http_code[%d], error_code[%s], error_msg[%s].",
                  sourceObjectStr.c_str(),
                  dstObjectStr.c_str(),
                  s->code,
                  s->error_code,
                  s->error_msg);
        ret = EC_UNKNOWN;
    }
    aos_pool_destroy(p);
    return ret;
}

ErrorCode OssFileSystem::renameObject(const string &accessKeyId,
                                      const string &accessKeySecret,
                                      const string &endpoint,
                                      const string &sourceBucketStr,
                                      const string &dstBucketStr,
                                      const string &sourceObjectStr,
                                      const string &dstObjectStr,
                                      bool hasSubFile) {
    ErrorCode ret;
    // 如果目标是目录，并且有子文件，则拷贝子文件的时候 该目录已经存在了，不需要在拷贝
    if (!hasSubFile) {
        ret = copyObject(
            accessKeyId, accessKeySecret, endpoint, sourceBucketStr, dstBucketStr, sourceObjectStr, dstObjectStr);
        if (ret != EC_OK) {
            return ret;
        }
    }
    ret = deleteObject(accessKeyId, accessKeySecret, endpoint, sourceBucketStr, sourceObjectStr);
    return ret;
}

ErrorCode OssFileSystem::getFileMeta(const string &fileName, FileMeta &fileMeta) {
    string bucket;
    string object;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    if (!parsePath(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }

    OssFile file(fileName, bucket, object, accessKeyId, accessKeySecret, endpoint, READ, EC_OK);
    if (file.initFileInfo() == EC_OK) {
        fileMeta = file.getFileMeta();
        return EC_OK;
    } else {
        ErrorCode ec = isDirectory(fileName);
        if (ec == EC_TRUE) {
            // yytodo get PathMeta
            memset(&fileMeta, 0, sizeof(fileMeta));
            return EC_OK;
        } else if (isExist(fileName) == EC_FALSE) {
            return EC_NOENT;
        }
        AUTIL_LOG(ERROR, "isDirectory [%s] for getFileMeta failed, ec[%d]", fileName.c_str(), ec);
        return ec;
    }
}

ErrorCode OssFileSystem::isFile(const string &path) {
    string bucket;
    string object;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    if (!parsePath(path, bucket, object, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }
    string dirPath = object;
    if (*path.rbegin() != '/') {
        OssFile file(path, bucket, object, accessKeyId, accessKeySecret, endpoint, READ, EC_OK);
        ErrorCode ec = file.initFileInfo();
        if (ec == EC_OK) {
            return EC_TRUE;
        } else if (ec != EC_NOENT) {
            return ec;
        }
        dirPath += "/";
    }
    OssFile dir(path, bucket, dirPath, accessKeyId, accessKeySecret, endpoint, READ, EC_OK);
    ErrorCode ec = dir.initFileInfo();
    if (ec == EC_OK) {
        return EC_FALSE;
    } else if (ec == EC_NOENT) {
        string prefix = dirPath;
        vector<string> files;
        vector<string> dirs;
        if (*prefix.rbegin() != '/') {
            prefix += "/";
        }
        // if it has sub file or dirs, regard it as dir
        ErrorCode ec = listObjects(accessKeyId, accessKeySecret, endpoint, bucket, prefix, "/", 1000, files, dirs);
        if (ec != EC_OK) {
            return ec;
        }
        if (files.size() > 0 || dirs.size() > 0) {
            return EC_FALSE;
        } else {
            return EC_NOENT;
        }
    }
    return EC_UNKNOWN;
}

FileChecksum OssFileSystem::getFileChecksum(const string &fileName) { return EC_NOTSUP; }

ErrorCode OssFileSystem::mkDir(const string &dirName, bool recursive) {
    // 判断是否存在
    if (isDirectory(dirName) == EC_TRUE) {
        return EC_EXIST;
    }
    if (isFile(dirName) == EC_TRUE) {
        return EC_EXIST;
    }

    string bucket;
    string object;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;

    if (!parsePath(dirName, bucket, object, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }
    if (*object.rbegin() != '/') {
        object += "/";
    }
    return mkDirObject(accessKeyId, accessKeySecret, endpoint, bucket, object);
}

ErrorCode OssFileSystem::mkDirObject(const string &accessKeyId,
                                     const string &accessKeySecret,
                                     const string &endpoint,
                                     const string &bucketStr,
                                     const string &objectStr) {
    aos_pool_t *p = NULL;
    aos_string_t bucket;
    aos_string_t object;
    oss_request_options_t *options = NULL;
    int is_cname = 0;
    aos_status_t *s = NULL;
    aos_list_t buffer;
    aos_table_t *headers = NULL;
    aos_table_t *resp_headers = NULL;
    ErrorCode ret;

    // init pool
    aos_pool_create(&p, NULL);
    options = oss_request_options_create(p);

    // init config
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->endpoint, endpoint.c_str());
    aos_str_set(&options->config->access_key_id, accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, accessKeySecret.c_str());
    options->config->is_cname = is_cname;
    options->ctl = aos_http_controller_create(options->pool, 0);
    aos_str_set(&bucket, bucketStr.c_str());
    aos_str_set(&object, objectStr.c_str());

    headers = aos_table_make(options->pool, 0);

    aos_list_init(&buffer);

    s = oss_put_object_from_buffer(options, &bucket, &object, &buffer, headers, &resp_headers);

    if (aos_status_is_ok(s)) {
        ret = EC_OK;
    } else {
        AUTIL_LOG(ERROR,
                  "oss mkdir object failed, http_code[%d], error_code[%s], error_msg[%s].",
                  s->code,
                  s->error_code,
                  s->error_msg);
        ret = EC_UNKNOWN;
    }

    aos_pool_destroy(p);
    return ret;
}

ErrorCode OssFileSystem::listObjects(const string &accessKeyId,
                                     const string &accessKeySecret,
                                     const string &endpoint,
                                     const string &bucketStr,
                                     const string &prefix,
                                     const string &delimiter,
                                     int32_t maxRet,
                                     vector<string> &contents,
                                     vector<string> &commonPrefixs) {
    aos_pool_t *p = NULL;
    aos_string_t bucket;
    oss_request_options_t *options = NULL;
    int is_cname = 0;
    aos_status_t *s = NULL;
    oss_list_object_params_t *params = NULL;
    oss_list_object_content_t *content = NULL;
    oss_list_object_common_prefix_t *commonPrefix = NULL;
    int size = 0;
    char const *nextMarker = "";

    // init pool
    aos_pool_create(&p, NULL);
    options = oss_request_options_create(p);

    // init config
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->endpoint, endpoint.c_str());
    aos_str_set(&options->config->access_key_id, accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, accessKeySecret.c_str());
    options->config->is_cname = is_cname;
    options->ctl = aos_http_controller_create(options->pool, 0);
    aos_str_set(&bucket, bucketStr.c_str());

    // init params
    params = oss_create_list_object_params(p);
    params->max_ret = maxRet;
    aos_str_set(&params->prefix, prefix.c_str());
    aos_str_set(&params->marker, nextMarker);
    aos_str_set(&params->delimiter, delimiter.c_str());

    do {
        s = oss_list_object(options, &bucket, params, NULL);
        if (!aos_status_is_ok(s)) {
            AUTIL_LOG(ERROR,
                      "oss list object failed, http_code[%d], error_code[%s], error_msg[%s].",
                      s->code,
                      s->error_code,
                      s->error_msg);
            return EC_UNKNOWN;
        }

        aos_list_for_each_entry(oss_list_object_content_t, content, &params->object_list, node) {
            if (prefix != content->key.data) {
                ++size;
                contents.push_back(string(content->key.data).substr(prefix.length()));
            }
        }

        aos_list_for_each_entry(oss_list_object_common_prefix_t, commonPrefix, &params->common_prefix_list, node) {
            if (prefix != commonPrefix->prefix.data) {
                ++size;
                string normalizedCommonPrefix =
                    string(commonPrefix->prefix.data)
                        .substr(prefix.length(),
                                commonPrefix->prefix.len - prefix.length() - 1); // remove prefix and last '/'
                commonPrefixs.push_back(normalizedCommonPrefix);
            }
        }

        nextMarker = apr_psprintf(p, "%.*s", params->next_marker.len, params->next_marker.data);
        aos_str_set(&params->marker, nextMarker);
        aos_list_init(&params->object_list);
        aos_list_init(&params->common_prefix_list);
    } while (params->truncated == AOS_TRUE);
    aos_pool_destroy(p);
    return EC_OK;
}

ErrorCode OssFileSystem::listDir(const string &dirName, FileList &fileList) {
    string bucket;
    string prefix;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    if (!parsePath(dirName, bucket, prefix, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }
    vector<string> files;
    vector<string> dirs;
    if (*prefix.rbegin() != '/') {
        prefix += "/";
    }
    listObjects(accessKeyId, accessKeySecret, endpoint, bucket, prefix, "/", 1000, files, dirs);
    fileList.clear();
    fileList.insert(fileList.end(), files.begin(), files.end());
    fileList.insert(fileList.end(), dirs.begin(), dirs.end());
    sort(fileList.begin(), fileList.end());
    return EC_OK;
}

ErrorCode OssFileSystem::isDirectory(const string &path) {
    ErrorCode ec = isFile(path);
    if (ec == EC_FALSE) {
        return EC_TRUE;
    }
    if (ec == EC_TRUE) {
        return EC_FALSE;
    }

    string bucket;
    string prefix;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    string dir;
    if (!parsePath(path, bucket, prefix, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }
    vector<string> files;
    vector<string> dirs;
    if (*prefix.rbegin() == '/') {
        prefix = prefix.substr(0, prefix.size() - 1);
    }

    string::size_type lastDirPos = prefix.rfind("/");
    if (lastDirPos != string::npos) {
        dir = prefix.substr(lastDirPos + 1, string::npos);
        prefix = prefix.substr(0, lastDirPos + 1);
    }
    ec = listObjects(accessKeyId, accessKeySecret, endpoint, bucket, prefix, "/", 1000, files, dirs);
    if (ec != EC_OK) {
        return ec;
    }

    if (find(dirs.begin(), dirs.end(), dir) != dirs.end()) {
        return EC_TRUE;
    }
    return EC_UNKNOWN;
}

ErrorCode OssFileSystem::deleteObject(const string &accessKeyId,
                                      const string &accessKeySecret,
                                      const string &endpoint,
                                      const string &bucketStr,
                                      const string &objectStr) {
    ErrorCode errorCode = EC_OK;
    aos_pool_t *p = NULL;
    aos_string_t bucket;
    aos_string_t object;
    oss_request_options_t *options = NULL;
    int is_cname = 0;
    aos_status_t *s = NULL;
    aos_table_t *resp_headers = NULL;

    // init pool
    aos_pool_create(&p, NULL);
    options = oss_request_options_create(p);

    // init config
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->endpoint, endpoint.c_str());
    aos_str_set(&options->config->access_key_id, accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, accessKeySecret.c_str());
    options->config->is_cname = is_cname;
    options->ctl = aos_http_controller_create(options->pool, 0);
    aos_str_set(&bucket, bucketStr.c_str());
    aos_str_set(&object, objectStr.c_str());

    s = oss_delete_object(options, &bucket, &object, &resp_headers);

    if (!aos_status_is_ok(s)) {
        AUTIL_LOG(ERROR,
                  "oss delete object[%s] failed "
                  "http_code[%d], error_code[%s], error_msg[%s]",
                  objectStr.c_str(),
                  s->code,
                  s->error_code,
                  s->error_msg);
        errorCode = EC_UNKNOWN;
    }

    aos_pool_destroy(p);
    return errorCode;
}

ErrorCode OssFileSystem::deleteObjects(const string &accessKeyId,
                                       const string &accessKeySecret,
                                       const string &endpoint,
                                       const string &bucketStr,
                                       const vector<string> &objects) {
    ErrorCode errorCode = EC_OK;
    aos_pool_t *p = NULL;
    aos_list_t object_list;
    aos_list_t deleted_object_list;
    aos_table_t *resp_headers = NULL;
    int is_quiet = AOS_TRUE;

    aos_list_init(&object_list);
    aos_list_init(&deleted_object_list);

    // init pool
    aos_pool_create(&p, NULL);
    oss_request_options_t *options = oss_request_options_create(p);

    // init config
    options->config = oss_config_create(options->pool);
    aos_str_set(&options->config->endpoint, endpoint.c_str());
    aos_str_set(&options->config->access_key_id, accessKeyId.c_str());
    aos_str_set(&options->config->access_key_secret, accessKeySecret.c_str());
    options->config->is_cname = 0;
    options->ctl = aos_http_controller_create(options->pool, 0);

    aos_string_t bucket;
    aos_str_set(&bucket, bucketStr.c_str());

    for (auto &&object : objects) {
        oss_object_key_t *content = oss_create_oss_object_key(p);
        aos_str_set(&content->key, object.c_str());
        aos_list_add_tail(&content->node, &object_list);
    }

    aos_status_t *s = oss_delete_objects(options, &bucket, &object_list, is_quiet, &resp_headers, &deleted_object_list);

    if (!aos_status_is_ok(s)) {
        AUTIL_LOG(ERROR,
                  "oss delete objects failed "
                  "http_code[%d], error_code[%s], error_msg[%s]",
                  s->code,
                  s->error_code,
                  s->error_msg);
        errorCode = EC_UNKNOWN;
    }

    aos_pool_destroy(p);
    return errorCode;
}

ErrorCode OssFileSystem::remove(const string &pathName) {
    string bucket;
    string object;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;

    if (!parsePath(pathName, bucket, object, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }

    if (isDirectory(pathName) == EC_TRUE) {
        if (*object.rbegin() != '/') {
            object += "/";
        }

        // DELETE ALL SUB OBJECTS IN DIRECTORY
        vector<string> files;
        vector<string> dirs;
        ErrorCode ec = listObjects(accessKeyId, accessKeySecret, endpoint, bucket, object, "", 1000, files, dirs);
        if (ec != EC_OK) {
            return ec;
        }

        vector<string> objects;
        for (size_t i = 0; i < files.size(); ++i) {
            string fileName = object + files[i];
            objects.push_back(fileName);
            AUTIL_LOG(INFO, "delete file %s", fileName.c_str());
        }

        for (size_t i = 0; i < dirs.size(); ++i) {
            string dirName = object + dirs[i];
            if (*dirName.rbegin() != '/') {
                dirName += "/";
            }
            objects.push_back(dirName);
            AUTIL_LOG(INFO, "delete dir %s", dirName.c_str());
        }

        if (objects.size() > 0) {
            ec = deleteObjects(accessKeyId, accessKeySecret, endpoint, bucket, objects);
            if (ec != EC_OK) {
                return ec;
            }
        }
    }

    return deleteObject(accessKeyId, accessKeySecret, endpoint, bucket, object);
}

ErrorCode OssFileSystem::isExist(const string &pathName) {
    ErrorCode ec = isFile(pathName);
    if (ec == EC_TRUE || ec == EC_FALSE) {
        return EC_TRUE;
    }
    if (ec == EC_NOENT) {
        return EC_FALSE;
    }
    return ec;
}

bool lessEntryMeta(EntryMeta a, EntryMeta b) { return a.entryName < b.entryName; }

ErrorCode OssFileSystem::listDir(const string &dirName, EntryList &entryList) {
    string bucket;
    string prefix;
    string accessKeyId;
    string accessKeySecret;
    string endpoint;
    if (!parsePath(dirName, bucket, prefix, accessKeyId, accessKeySecret, endpoint)) {
        return EC_BADARGS;
    }
    vector<string> files;
    vector<string> dirs;
    if (*prefix.rbegin() != '/') {
        prefix += "/";
    }
    ErrorCode ec = listObjects(accessKeyId, accessKeySecret, endpoint, bucket, prefix, "/", 1000, files, dirs);
    if (ec != EC_OK) {
        return ec;
    }
    entryList.clear();
    for (size_t i = 0; i < files.size(); i++) {
        EntryMeta entryMeta;
        entryMeta.isDir = false;
        entryMeta.entryName = files[i];
        entryList.push_back(entryMeta);
    }
    for (size_t i = 0; i < dirs.size(); i++) {
        EntryMeta entryMeta;
        entryMeta.isDir = true;
        entryMeta.entryName = dirs[i];
        entryList.push_back(entryMeta);
    }
    sort(entryList.begin(), entryList.end(), lessEntryMeta);
    return EC_OK;
}

string OssFileSystem::normalizePath(const string &path) {
    string ret = path;
    if (*ret.rbegin() != '/') {
        ret += "/";
    }
    return ret;
}

FSLIB_PLUGIN_END_NAMESPACE(oss);
