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
#include "build_service/admin/controlflow/LocalLuaScriptReader.h"

#include "autil/EnvUtil.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/GraphConfig.h"
#include "build_service/util/ZipFileUtil.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace build_service::util;
using namespace build_service::common;
using namespace build_service::config;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, LocalLuaScriptReader);

bool LocalLuaScriptReader::RedirectInfo::isRedirectInfo(const string& str)
{
    try {
        Any any = ParseJson(str);
        JsonMap jsonMap = AnyCast<JsonMap>(any);
        auto iter = jsonMap.find("redirect_to");
        return iter != jsonMap.end();
    } catch (const ExceptionBase& e) {
        return false;
    }
    return false;
}

LocalLuaScriptReader::LocalLuaScriptReader() : _totalInnerFileLen(0) {}

LocalLuaScriptReader::~LocalLuaScriptReader() {}

bool LocalLuaScriptReader::getEnvLuaPathInfo(string& luaPath, ConfigInfo& configInfo)
{
    string luaConfigPath = autil::EnvUtil::getEnv(BS_ENV_LUA_CONFIG_PATH);
    if (luaConfigPath.empty()) {
        BS_LOG(WARN, "get env[%s] fail, invalid path [%s]", BS_ENV_LUA_CONFIG_PATH.c_str(), luaConfigPath.c_str());
        string str = autil::EnvUtil::getEnv("HIPPO_APP_INST_ROOT");
        string usrLocalPath;
        if (!str.empty()) {
            usrLocalPath = str;
            usrLocalPath += "/usr/local/";
        } else {
            char buffer[1024];
            int ret = readlink("/proc/self/exe", buffer, sizeof(buffer) - 1);
            // .../usr/local/bin
            usrLocalPath =
                fslib::util::FileUtil::getParentDir(fslib::util::FileUtil::getParentDir(string(buffer, ret)));
        }
        // .../usr/local/etc/bs/lua/lua.conf
        luaConfigPath = fslib::util::FileUtil::joinFilePath(usrLocalPath, "etc/bs/lua/lua.conf");
    }

    string configInfoStr;
    if (!fslib::util::FileUtil::readFile(luaConfigPath, configInfoStr)) {
        BS_LOG(ERROR, "read lua config file [%s] fail", luaConfigPath.c_str());
        return false;
    }

    try {
        autil::legacy::FromJsonString(configInfo, configInfoStr);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", configInfoStr.c_str(), e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", configInfoStr.c_str());
        return false;
    }

    string luaRootDir = fslib::util::FileUtil::getParentDir(luaConfigPath);
    luaPath = fslib::util::FileUtil::joinFilePath(luaRootDir, configInfo.pathId);

    string actualPath = fslib::util::FileUtil::joinFilePath(luaRootDir, configInfo.isZip ? configInfo.pathId + ".zip"
                                                                                         : configInfo.pathId);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(actualPath, exist) || !exist) {
        BS_LOG(ERROR, "fail to check lua script path [%s] existance!", actualPath.c_str());
        return false;
    }
    return true;
}

bool LocalLuaScriptReader::initLocalPath(const string& zkRoot)
{
    string luaPath;
    ConfigInfo confInfo;
    if (!getEnvLuaPathInfo(luaPath, confInfo)) {
        return false;
    }

    if (!syncEnvLuaPathToZk(luaPath, confInfo, zkRoot)) {
        return false;
    }
    return downloadFromZkLuaRoot(zkRoot);
}

bool LocalLuaScriptReader::downloadFromZkLuaRoot(const string& zkRoot)
{
    string luaZkRoot = common::PathDefine::getAdminControlFlowRoot(zkRoot);
    bool exist = false;
    if (fslib::util::FileUtil::isExist(luaZkRoot, exist) && !exist) {
        BS_LOG(INFO, "path [%s] for bs zk control script root not exist!", luaZkRoot.c_str());
        return true;
    }

    vector<string> fileList;
    if (!fslib::util::FileUtil::listDir(luaZkRoot, fileList)) {
        BS_LOG(ERROR, "list file path [%s] fail!", luaZkRoot.c_str());
        return false;
    }
    for (auto& path : fileList) {
        ConfigInfo info = ConfigInfo::parse(path);
        if (!copyLuaPath(luaZkRoot, info, PathDefine::getLocalLuaScriptRootPath())) {
            return false;
        }

        bool useZip = false;
        if (!isLocalLuaPathExist(info.pathId, useZip)) {
            return false;
        }
        if (useZip && !preloadZipLuaPath(info.pathId)) {
            return false;
        }
    }
    return true;
}

bool LocalLuaScriptReader::syncEnvLuaPathToZk(const string& envLuaPath, const ConfigInfo& configInfo,
                                              const string& zkRoot)
{
    string luaZkRoot = common::PathDefine::getAdminControlFlowRoot(zkRoot);
    bool exist = false;
    if (fslib::util::FileUtil::isExist(luaZkRoot, exist) && !exist) {
        if (!fslib::util::FileUtil::mkDir(luaZkRoot, true)) {
            BS_LOG(ERROR, "mkdir [%s] fail!", luaZkRoot.c_str());
            return false;
        }
        BS_LOG(INFO, "mkdir [%s] for bs zk control script root", luaZkRoot.c_str());
    }
    string envLuaRoot = envLuaPath.substr(0, envLuaPath.length() - configInfo.pathId.length());
    return copyLuaPath(envLuaRoot, configInfo, luaZkRoot);
}

bool LocalLuaScriptReader::reuseLocalLuaPath()
{
    bool reuseLocalLuaDir = autil::EnvUtil::getEnv("reuse_local_lua_dir", false);
    if (reuseLocalLuaDir) {
        BS_LOG(INFO, "enable reuse_local_lua_dir env for DEBUG purpose!");
    }
    return reuseLocalLuaDir;
}

bool LocalLuaScriptReader::copyLuaPath(const string& srcPath, const ConfigInfo& configInfo, const string& dstPath)
{
    string dstCopyDir = fslib::util::FileUtil::joinFilePath(dstPath, configInfo.pathId);
    if (!reuseLocalLuaPath() && !fslib::util::FileUtil::removeIfExist(dstCopyDir)) {
        BS_LOG(ERROR, "remove [%s] fail!", dstCopyDir.c_str());
        return false;
    }
    dstCopyDir = dstCopyDir + ".zip";
    if (!fslib::util::FileUtil::removeIfExist(dstCopyDir)) {
        BS_LOG(ERROR, "remove [%s] fail!", dstCopyDir.c_str());
        return false;
    }

    if (configInfo.isZip) {
        string srcFilePath = fslib::util::FileUtil::joinFilePath(srcPath, configInfo.pathId) + ".zip";
        string destFilePath = fslib::util::FileUtil::joinFilePath(dstPath, configInfo.pathId) + ".zip";
        if (!fslib::util::FileUtil::atomicCopy(srcFilePath, destFilePath)) {
            BS_LOG(ERROR, "copy from [%s] to [%s] fail", srcFilePath.c_str(), destFilePath.c_str());
            return false;
        }
        BS_LOG(INFO, "copy from [%s] to [%s] success", srcFilePath.c_str(), destFilePath.c_str());
        return true;
    }

    string dstLuaPath = fslib::util::FileUtil::joinFilePath(dstPath, configInfo.pathId);
    bool exist = false;
    if (fslib::util::FileUtil::isExist(dstLuaPath, exist) && exist) {
        BS_LOG(INFO, "local lua path [%s] already exist, reuse it for DEBUG purpose!", dstLuaPath.c_str());
        return true;
    }
    // copy dir
    string srcLuaPath = fslib::util::FileUtil::joinFilePath(srcPath, configInfo.pathId);
    vector<string> entryVec;
    if (!fslib::util::FileUtil::listDirRecursive(srcLuaPath, entryVec, true)) {
        BS_LOG(ERROR, "list file path [%s] fail!", srcLuaPath.c_str());
        return false;
    }

    exist = false;
    if (fslib::util::FileUtil::isExist(dstLuaPath, exist) && !exist) {
        fslib::util::FileUtil::mkDir(dstLuaPath, true);
    }
    for (auto& file : entryVec) {
        string srcFilePath = fslib::util::FileUtil::joinFilePath(srcLuaPath, file);
        string dstFilePath = fslib::util::FileUtil::joinFilePath(dstLuaPath, file);
        if (!fslib::util::FileUtil::atomicCopy(srcFilePath, dstFilePath)) {
            BS_LOG(ERROR, "copy from [%s] to [%s] fail", srcFilePath.c_str(), dstFilePath.c_str());
            return false;
        }
        BS_LOG(INFO, "copy from [%s] to [%s] success", srcFilePath.c_str(), dstFilePath.c_str());
    }

    BS_LOG(INFO, "copy from [%s] to [%s] success", srcLuaPath.c_str(), dstLuaPath.c_str());
    return true;
}

string LocalLuaScriptReader::getLocalLuaPath(const string& luaPathId)
{
    string localLuaRoot = PathDefine::getLocalLuaScriptRootPath();
    return fslib::util::FileUtil::joinFilePath(localLuaRoot, luaPathId);
}

bool LocalLuaScriptReader::isLocalLuaPathExist(const string& luaPathId, bool& zip)
{
    zip = false;
    string localLuaRoot = PathDefine::getLocalLuaScriptRootPath();
    string localLuaPath = fslib::util::FileUtil::joinFilePath(localLuaRoot, luaPathId);
    bool exist = false;
    if (fslib::util::FileUtil::isDir(localLuaPath, exist) && exist) {
        // dir exist
        zip = false;
        return true;
    }

    ConfigInfo luaInfo(luaPathId, true);
    string localLuaZipFile =
        fslib::util::FileUtil::joinFilePath(localLuaRoot, luaInfo.isZip ? luaInfo.pathId + ".zip" : luaInfo.pathId);
    if (fslib::util::FileUtil::isExist(localLuaZipFile, exist) && exist) {
        zip = true;
        return true;
    }
    return false;
}

string LocalLuaScriptReader::getScriptFilePath(const string& rootPath, const string& fileName)
{
    vector<string> pathVec;
    StringUtil::fromString(rootPath, pathVec, ";");
    if (pathVec.size() == 1) {
        return fslib::util::FileUtil::joinFilePath(pathVec[0], fileName);
    }

    for (auto& path : pathVec) {
        // multi root path
        string filePath = fslib::util::FileUtil::joinFilePath(path, fileName);
        if (isExist(filePath)) {
            return filePath;
        }
    }
    return fileName;
}

string LocalLuaScriptReader::resetRootPathWithGraphNamespace(const string& rootPath, const string& nsPath)
{
    vector<string> retVec;
    vector<string> pathVec;
    StringUtil::fromString(rootPath, pathVec, ";");
    retVec.reserve(pathVec.size() * 2);

    for (auto& path : pathVec) {
        retVec.push_back(fslib::util::FileUtil::joinFilePath(path, nsPath));
    }
    for (auto& path : pathVec) {
        retVec.push_back(path);
    }
    return StringUtil::toString(retVec, ";");
}

bool LocalLuaScriptReader::preloadZipLuaPath(const string& luaPathId)
{
    string zipLuaFile = getLocalLuaPath(luaPathId) + ".zip";
    return LocalLuaScriptReader::GetInstance()->loadZipLuaFile(zipLuaFile);
}

bool LocalLuaScriptReader::isExist(const string& filePath)
{
    if (LocalLuaScriptReader::GetInstance()->isExistInZip(filePath)) {
        return true;
    }
    bool exist;
    return fslib::util::FileUtil::isExist(filePath, exist) && exist;
}

bool LocalLuaScriptReader::readFile(const string& filePath, string& content)
{
    content.clear();
    if (LocalLuaScriptReader::GetInstance()->readFileInZip(filePath, content)) {
        return true;
    }
    return fslib::util::FileUtil::readFile(filePath, content);
}

bool LocalLuaScriptReader::isExistInZip(const string& filePath) const
{
    ScopedLock lock(_lock);
    return _zipInnerFiles.find(filePath) != _zipInnerFiles.end();
}

bool LocalLuaScriptReader::readFileInZip(const string& filePath, string& content) const
{
    ScopedLock lock(_lock);
    auto iter = _zipInnerFiles.find(filePath);
    if (iter == _zipInnerFiles.end()) {
        return false;
    }
    content = iter->second;
    return true;
}

bool LocalLuaScriptReader::loadZipLuaFile(const string& zipLuaFile)
{
    size_t pos = zipLuaFile.find(".zip");
    if (pos == string::npos) {
        BS_LOG(ERROR, "invalid zip file [%s]", zipLuaFile.c_str());
        return false;
    }

    {
        ScopedLock lock(_lock);
        if (_zipFileSet.find(zipLuaFile) != _zipFileSet.end()) {
            BS_LOG(INFO, "lua zip file [%s] already in reader cache!", zipLuaFile.c_str());
            return true;
        }
    }

    unordered_map<string, string> result;
    if (!ZipFileUtil::readZipFile(zipLuaFile, result)) {
        BS_LOG(ERROR, "read zip file [%s] fail!", zipLuaFile.c_str());
        return false;
    }

    ScopedLock lock(_lock);
    string pathPrefix = zipLuaFile.substr(0, pos);
    for (auto& item : result) {
        string innerFilePath = fslib::util::FileUtil::joinFilePath(pathPrefix, item.first);
        auto ret = _zipInnerFiles.insert(make_pair(innerFilePath, item.second));
        if (!ret.second) {
            BS_LOG(WARN, "file [%s] already exist in cache!", innerFilePath.c_str());
            continue;
        }
        _totalInnerFileLen += item.second.length();
    }
    _zipFileSet.insert(zipLuaFile);

    BS_LOG(INFO, "load zip lua file [%s] success, total cache file length [%lu]", zipLuaFile.c_str(),
           _totalInnerFileLen);
    return true;
}

bool LocalLuaScriptReader::readScriptFile(const TaskResourceManagerPtr& resMgr, const string& rootPath,
                                          const string& fileName, string& actualFilePath, string& content)
{
    ResourceReaderPtr reader;
    if (resMgr) {
        ConfigReaderAccessorPtr configAccessor;
        resMgr->getResource(configAccessor);
        if (configAccessor) {
            reader = configAccessor->getLatestConfig();
        }
    }

    if (!doReadScriptFile(reader, rootPath, fileName, actualFilePath, content)) {
        return false;
    }

    while (RedirectInfo::isRedirectInfo(content)) {
        RedirectInfo info;
        FromJsonString(info, content);
        if (info.isAbsPath) {
            BS_LOG(INFO, "[%s] redirect to absolute path [%s]", actualFilePath.c_str(), info.redirectFileName.c_str());
            actualFilePath = info.redirectFileName;
            if (LocalLuaScriptReader::readFile(actualFilePath, content)) {
                return true;
            }
            BS_LOG(ERROR, "read script from file [%s] fail", actualFilePath.c_str());
            return false;
        }
        BS_LOG(INFO, "[%s] redirect to [%s]", actualFilePath.c_str(), info.redirectFileName.c_str());
        if (!doReadScriptFile(reader, rootPath, info.redirectFileName, actualFilePath, content)) {
            return false;
        }
    }
    return true;
}

string LocalLuaScriptReader::getRelativeScriptFileName(const string& filePath, const string& rootPath)
{
    vector<string> pathVec;
    StringUtil::fromString(rootPath, pathVec, ";");
    string ret;
    for (auto& path : pathVec) {
        if (filePath.find(path) == 0) {
            ret = filePath.substr(path.length(), filePath.length() - path.length());
            break;
        }
    }
    return ret;
}

bool LocalLuaScriptReader::doReadScriptFile(const ResourceReaderPtr& reader, const string& rootPath,
                                            const string& fileName, string& actualFilePath, string& content)
{
    string nsPath = "";
    content.clear();
    if (reader) {
        config::GraphConfig graphConfig;
        if (reader->getGraphConfig(graphConfig)) {
            nsPath = graphConfig.getGraphPath();
        }
        string relativeFilePath = fslib::util::FileUtil::joinFilePath(nsPath, fileName);
        string relativeLuaPath = reader->getLuaScriptFileRelativePath(relativeFilePath);
        if (reader->getLuaFileContent(relativeLuaPath, content)) {
            actualFilePath = fslib::util::FileUtil::joinFilePath(reader->getConfigPath(), relativeLuaPath);
            return true;
        }
    }

    if (nsPath.empty()) {
        actualFilePath = LocalLuaScriptReader::getScriptFilePath(rootPath, fileName);
    } else {
        string newRoot = LocalLuaScriptReader::resetRootPathWithGraphNamespace(rootPath, nsPath);
        actualFilePath = LocalLuaScriptReader::getScriptFilePath(newRoot, fileName);
    }
    if (LocalLuaScriptReader::readFile(actualFilePath, content)) {
        return true;
    }
    BS_LOG(ERROR, "read script from file [%s] fail", actualFilePath.c_str());
    return false;
}

}} // namespace build_service::admin
