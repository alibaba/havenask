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
#include "fslib/cache/FSCacheModule.h"

#include <unistd.h>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/cache/FileDataCache.h"
#include "fslib/cache/PathMetaCache.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace fslib::fs;
FSLIB_USE_NAMESPACE(util);

FSLIB_BEGIN_NAMESPACE(cache);
AUTIL_LOG_SETUP(cache, FSCacheModule);

const int64_t FSCacheModule::DEFAULT_CACHE_SINGLE_FILE_THRESHOLD = 64 * 1024;
const int64_t FSCacheModule::MAX_CACHE_SINGLE_FILE_THRESHOLD = 4 * 1024 * 1024;
const int64_t FSCacheModule::DEFAULT_TOTAL_DATA_CACHE_SIZE = 32 * 1024 * 1024;

FSCacheModule::PatternMatcher::PatternMatcher() : _mode(MM_DISABLE) {}

FSCacheModule::PatternMatcher::~PatternMatcher() {}

void FSCacheModule::PatternMatcher::init(const string &mode, const vector<string> &patterns) {
    if (patterns.empty() || mode == "disable") {
        _mode = MM_DISABLE;
        return;
    }
    if (mode == "whitelist") {
        _mode = MM_WHITE;
    }
    if (mode == "blacklist") {
        _mode = MM_BLACK;
    }
    for (size_t i = 0; i < patterns.size(); i++) {
        RegularExprPtr expr(new RegularExpr);
        if (expr->init(patterns[i])) {
            _regExprVec.push_back(expr);
        }
    }
}

bool FSCacheModule::PatternMatcher::match(const string &filePath) const {
    if (_mode == MM_DISABLE) {
        return true;
    }
    bool match = false;
    for (size_t i = 0; i < _regExprVec.size(); i++) {
        if (_regExprVec[i]->match(filePath)) {
            match = true;
            break;
        }
    }
    if (_mode == MM_WHITE) {
        return match;
    }
    assert(_mode == MM_BLACK);
    return !match;
}

FSCacheModule::FSCacheModule() : _cacheFileThreshold(DEFAULT_CACHE_SINGLE_FILE_THRESHOLD) { initFromEnv(); }

FSCacheModule::~FSCacheModule() {}

void FSCacheModule::initFromEnv() {
    initCacheSupportFsTypes();
    if (_supportFsTypeSet.empty()) {
        return;
    }

    initCacheFileMatchPatterns();
    initFslibDataCache();
    initFslibMetaCache();
}

bool FSCacheModule::addPathMeta(
    const string &filePath, int64_t fileLength, uint64_t createTime, uint64_t modifyTime, bool isDirectory) {
    string normPath = PathUtil::normalizePath(filePath);
    if (!needCacheMeta(normPath)) {
        return false;
    }

    if (!_patternMatcher.match(normPath)) {
        return false;
    }
    _metaCache->addPathMeta(normPath, fileLength, createTime, modifyTime, isDirectory);

    FileSystem::reportMetaCachedPathCount(getMetaCachedPathCount());
    return true;
}

bool FSCacheModule::setImmutablePath(const string &immutablePath,
                                     const vector<pair<string, PathMeta>> &innerPathMetas) {
    string normPath = PathUtil::normalizePath(immutablePath);
    if (!needCacheMeta(normPath)) {
        return false;
    }

    bool hasFilter = false;
    for (auto p : innerPathMetas) {
        string path = PathUtil::normalizePath(PathUtil::joinPath(normPath, p.first));
        if (!_patternMatcher.match(path)) {
            hasFilter = true;
            continue;
        }
        _metaCache->addPathMeta(path, p.second.length, p.second.createTime, p.second.lastModifyTime, !p.second.isFile);
    }
    if (!hasFilter) {
        _metaCache->markImmutablePath(normPath, true);
        AUTIL_LOG(
            INFO, "mark immutable path [%s] with [%lu] inner path nodes", normPath.c_str(), innerPathMetas.size());
    }
    FileSystem::reportMetaCachedPathCount(getMetaCachedPathCount());
    FileSystem::reportMetaCacheImmutablePathCount(getMetaCacheImmutablePathCount());
    return true;
}

bool FSCacheModule::matchImmutablePath(const string &path) const {
    string normPath = PathUtil::normalizePath(path);
    if (!needCacheMeta(normPath)) {
        return false;
    }
    return _metaCache->matchImmutablePath(normPath);
}

bool FSCacheModule::getPathMeta(const string &filePath, PathMeta &pathMeta) const {
    string normPath = PathUtil::normalizePath(filePath);
    if (!needCacheMeta(normPath)) {
        return false;
    }
    if (!_metaCache->getPathMeta(normPath, pathMeta)) {
        return false;
    }
    FileSystem::reportMetaCacheHitQps(normPath);
    return true;
}

bool FSCacheModule::listInImmutablePath(const string &path,
                                        vector<pair<string, PathMeta>> &subFiles,
                                        bool recursive) const {
    string normPath = PathUtil::normalizePath(path);
    if (!needCacheMeta(normPath)) {
        return false;
    }
    if (!_metaCache->matchImmutablePath(normPath)) {
        return false;
    }
    if (!_metaCache->ListPathMetaInCache(normPath, subFiles, recursive)) {
        return false;
    }
    FileSystem::reportMetaCacheHitQps(normPath);
    return true;
}

void FSCacheModule::parseDataCacheParam(const string &dataCacheParam,
                                        int64_t &cacheFileThreshold,
                                        int64_t &totalCacheSize) {
    vector<vector<string>> kvPairs;
    StringUtil::fromString(dataCacheParam, kvPairs, "=", ",");
    for (auto &kvpair : kvPairs) {
        if (kvpair.size() != 2) {
            AUTIL_LOG(
                ERROR, "invalid kvpair [%s] in FSLIB_DATA_CACHE_PARAM", StringUtil::toString(kvpair, "=").c_str());
            cerr << "invalid kvpair [" << StringUtil::toString(kvpair, "=") << "] in FSLIB_DATA_CACHE_PARAM" << endl;
            continue;
        }
        if (kvpair[0] == "cache_file_threshold") {
            if (!StringUtil::fromString(kvpair[1], cacheFileThreshold)) {
                AUTIL_LOG(ERROR, "invalid cache_file_threshold [%s]", kvpair[1].c_str());
                cerr << "invalid cache_file_threshold [" << kvpair[1] << "]" << endl;
                continue;
            }
        }
        if (kvpair[0] == "total_cache_size") {
            if (!StringUtil::fromString(kvpair[1], totalCacheSize)) {
                AUTIL_LOG(ERROR, "invalid total_cache_size [%s]", kvpair[1].c_str());
                cerr << "invalid total_cache_size [" << kvpair[1] << "]" << endl;
                continue;
            }
        }
    }

    if (cacheFileThreshold > totalCacheSize) {
        AUTIL_LOG(INFO,
                  "cache_file_threshold [%ld] is bigger than total_cache_size [%ld]",
                  cacheFileThreshold,
                  totalCacheSize);
        cerr << "cache_file_threshold [" << cacheFileThreshold << "] is bigger than total_cache_size ["
             << totalCacheSize << "]" << endl;
        cacheFileThreshold = min(DEFAULT_CACHE_SINGLE_FILE_THRESHOLD, totalCacheSize);
    }
    if (cacheFileThreshold > MAX_CACHE_SINGLE_FILE_THRESHOLD) {
        AUTIL_LOG(INFO,
                  "cache_file_threshold [%ld] too big, set to max [%ld]",
                  DEFAULT_CACHE_SINGLE_FILE_THRESHOLD,
                  MAX_CACHE_SINGLE_FILE_THRESHOLD);
        cerr << "cache_file_threshold [" << DEFAULT_CACHE_SINGLE_FILE_THRESHOLD << "] too big, set to max ["
             << MAX_CACHE_SINGLE_FILE_THRESHOLD << "]" << endl;
        cacheFileThreshold = MAX_CACHE_SINGLE_FILE_THRESHOLD;
    }
}

void FSCacheModule::initCacheSupportFsTypes() {
    string supportFsType;
    string tmp = autil::EnvUtil::getEnv("FSLIB_CACHE_SUPPORTING_FS_TYPE");
    if (!tmp.empty()) {
        supportFsType = tmp;
    } else {
        supportFsType = "hdfs|pangu|dfs|jfs";
    }

    AUTIL_LOG(INFO, "fslib cache-supporting fsType [%s]", supportFsType.c_str());
    vector<string> fsTypes;
    StringUtil::fromString(supportFsType, fsTypes, "|");
    for (auto &type : fsTypes) {
        _supportFsTypeSet.insert(type);
    }
}

void FSCacheModule::initCacheFileMatchPatterns() {
    string cachePatternParam;
    string tmp = autil::EnvUtil::getEnv("FSLIB_CACHE_FILE_PATTERN");
    if (!tmp.empty()) {
        cachePatternParam = tmp;
    } else {
        cachePatternParam = "mode=disable";
    }

    AUTIL_LOG(INFO, "fslib cache file pattern param [%s]", cachePatternParam.c_str());
    string modeTypeStr = "disable";
    vector<string> patternVec;
    vector<vector<string>> kvPairs;
    StringUtil::fromString(cachePatternParam, kvPairs, "=", ",");
    for (auto kv : kvPairs) {
        if (kv.size() != 2) {
            AUTIL_LOG(ERROR, "invalid FSLIB_CACHE_FILE_PATTERN [%s]", cachePatternParam.c_str());
            cerr << "invalid FSLIB_CACHE_FILE_PATTERN [" << cachePatternParam << "]" << endl;
            modeTypeStr = "disable";
            patternVec.clear();
            break;
        }
        if (kv[0] == "mode") {
            modeTypeStr = kv[1];
            continue;
        }
        if (kv[0] == "pattern") {
            patternVec.push_back(kv[1]);
            continue;
        }
        AUTIL_LOG(ERROR, "invalid key [%s] in FSLIB_CACHE_FILE_PATTERN [%s]", kv[0].c_str(), cachePatternParam.c_str());
        cerr << "invalid key [" << kv[0] << "] in FSLIB_CACHE_FILE_PATTERN [" << cachePatternParam << "]" << endl;
        modeTypeStr = "disable";
        patternVec.clear();
        break;
    }

    if (modeTypeStr != "disable" && modeTypeStr != "blacklist" && modeTypeStr != "whitelist") {
        AUTIL_LOG(ERROR,
                  "invalid mode [%s] in FSLIB_CACHE_FILE_PATTERN [%s]",
                  modeTypeStr.c_str(),
                  cachePatternParam.c_str());
        cerr << "invalid mode [" << modeTypeStr << "] in FSLIB_CACHE_FILE_PATTERN [" << cachePatternParam << "]"
             << endl;

        modeTypeStr = "disable";
        patternVec.clear();
    }
    _patternMatcher.init(modeTypeStr, patternVec);
}

void FSCacheModule::initFslibDataCache() {
    int64_t cacheFileThreshold = DEFAULT_CACHE_SINGLE_FILE_THRESHOLD;
    int64_t totalCacheSize = DEFAULT_TOTAL_DATA_CACHE_SIZE;
    string useDataCache = autil::EnvUtil::getEnv("FSLIB_ENABLE_DATA_CACHE");
    if (useDataCache != "true") {
        return;
    }

    string dataCacheParam = autil::EnvUtil::getEnv("FSLIB_DATA_CACHE_PARAM");
    if (!dataCacheParam.empty()) {
        parseDataCacheParam(dataCacheParam, cacheFileThreshold, totalCacheSize);
    }

    AUTIL_LOG(
        INFO, "Enable DataCache, cacheFileThreshold [%ld], totalCacheSize [%ld]", cacheFileThreshold, totalCacheSize);

    _dataCache.reset(new FileDataCache(totalCacheSize));
    _cacheFileThreshold = cacheFileThreshold;
    FileSystem::reportDataCachedFileCount(0);
    FileSystem::reportDataCacheMemUse(0);
}

void FSCacheModule::initFslibMetaCache() {
    string useMetaCache = autil::EnvUtil::getEnv("FSLIB_ENABLE_META_CACHE");
    if (useMetaCache != "true") {
        return;
    }
    AUTIL_LOG(INFO, "Enable MetaCache");
    _metaCache.reset(new PathMetaCache);
    FileSystem::reportMetaCachedPathCount(0);
    FileSystem::reportMetaCacheImmutablePathCount(0);
}

bool FSCacheModule::getFileDataFromCache(const string &filePath, string &data) const {
    string normPath = PathUtil::normalizePath(filePath);
    if (!needCacheData(normPath)) {
        return false;
    }

    // TODO: parse timestamp to support ttl
    int64_t inCacheTs = -1;
    if (!_dataCache->get(normPath, data, inCacheTs)) {
        return false;
    }
    FileSystem::reportDataCacheHitQps(normPath);
    return true;
}

void FSCacheModule::putFileDataToCache(const string &filePath, File *file) {
    string normPath = PathUtil::normalizePath(filePath);
    if (!needCacheData(normPath) || !file || !file->isOpened()) {
        return;
    }

    string content;
    int64_t inCacheTs = -1;
    // TODO: parse timestamp to support ttl
    if (_dataCache->get(normPath, content, inCacheTs)) {
        return;
    }

    if (!_patternMatcher.match(normPath)) {
        return;
    }
    int64_t len = 0;
    if (!getFileLength(normPath, file, len)) {
        AUTIL_LOG(ERROR, "fail to get file length for [%s]", normPath.c_str());
        cerr << "fail to get file length for [" << normPath << "]" << endl;
        return;
    }

    if (len > _cacheFileThreshold) {
        return;
    }
    content.resize(len);
    ErrorCode ret = file->seek(0, FILE_SEEK_SET);
    if (ret != EC_OK) {
        return;
    }
    ssize_t rlen = file->read((void *)content.data(), len);
    if (rlen != len) {
        AUTIL_LOG(ERROR, "read fail [%ld:%ld] get file length for [%s]", rlen, len, normPath.c_str());
        cerr << "read fail [" << rlen << ":" << len << "] get file length for [" << normPath << "]" << endl;
        return;
    }
    _dataCache->put(normPath, content);
    FileSystem::reportDataCacheMemUse(getDataCacheMemUse());
    FileSystem::reportDataCachedFileCount(getDataCachedFileCount());
}

bool FSCacheModule::getFileLength(const string &filePath, File *file, int64_t &len) {
    string normPath = PathUtil::normalizePath(filePath);
    PathMeta pMeta;
    if (_metaCache && _metaCache->getPathMeta(normPath, pMeta)) {
        len = pMeta.length;
        return true;
    }

    assert(file && file->isOpened());
    if (!file->isEof()) {
        ErrorCode ret = file->seek(0, FILE_SEEK_END);
        if (ret != EC_OK) {
            return false;
        }
    }
    len = file->tell();
    return true;
}

void FSCacheModule::reset() {
    _supportFsTypeSet.clear();
    _dataCache.reset();
    _metaCache.reset();
    _cacheFileThreshold = DEFAULT_CACHE_SINGLE_FILE_THRESHOLD;
    initFromEnv();
}

void FSCacheModule::invalidatePath(const string &path) {
    string normPath = PathUtil::normalizePath(path);
    if (_metaCache) {
        PathMeta pMeta;
        if (_metaCache->getPathMeta(normPath, pMeta) && pMeta.isFile) {
            _metaCache->removeFile(normPath);
        } else {
            _metaCache->removeDirectory(normPath);
        }
        FileSystem::reportMetaCachedPathCount(getMetaCachedPathCount());
        FileSystem::reportMetaCacheImmutablePathCount(getMetaCacheImmutablePathCount());
    }

    if (_dataCache) {
        string content;
        int64_t ts = -1;
        if (_dataCache->get(normPath, content, ts)) {
            _dataCache->removeFile(normPath);
        } else {
            _dataCache->removeDirectory(normPath);
        }
        FileSystem::reportDataCacheMemUse(getDataCacheMemUse());
        FileSystem::reportDataCachedFileCount(getDataCachedFileCount());
    }
}

bool FSCacheModule::needCache(const string &normPath) const {
    FsType fsType = FileSystem::getFsType(normPath);
    return _supportFsTypeSet.find(fsType) != _supportFsTypeSet.end();
}

int64_t FSCacheModule::getMetaCachedPathCount() const {
    if (!_metaCache) {
        return 0;
    }
    return _metaCache->getCachedPathCount();
}

int64_t FSCacheModule::getMetaCacheImmutablePathCount() const {
    if (!_metaCache) {
        return 0;
    }
    return _metaCache->getImmutablePathCount();
}

int64_t FSCacheModule::getDataCacheMemUse() const {
    if (!_dataCache) {
        return 0;
    }
    return _dataCache->getCacheMemUse();
}

int64_t FSCacheModule::getDataCachedFileCount() const {
    if (!_dataCache) {
        return 0;
    }
    return _dataCache->getCachedFileCount();
}

FSLIB_END_NAMESPACE(cache);
