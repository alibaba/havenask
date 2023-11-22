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

#include <stddef.h>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "indexlib/util/Singleton.h"

namespace build_service { namespace admin {

class TaskResourceManager;

BS_TYPEDEF_PTR(TaskResourceManager);

class LocalLuaScriptReader : public indexlib::util::Singleton<LocalLuaScriptReader>
{
public:
    struct ConfigInfo : public autil::legacy::Jsonizable {
    public:
        ConfigInfo(const std::string& id = "", bool zip = false) : pathId(id), isZip(zip) {}

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("id", pathId);
            json.Jsonize("zip", isZip);
        }

        static ConfigInfo parse(const std::string& fileName)
        {
            if (fileName.length() < 4) {
                return ConfigInfo(fileName);
            }
            std::string zipSuffix = fileName.substr(fileName.length() - 4, 4);
            if (zipSuffix == ".zip") {
                return ConfigInfo(fileName.substr(0, fileName.length() - 4), true);
            }
            return ConfigInfo(fileName);
        }

    public:
        std::string pathId;
        bool isZip;
    };

    struct RedirectInfo : public autil::legacy::Jsonizable {
        RedirectInfo(const std::string& _fileName = "", bool _isAbsPath = false)
            : redirectFileName(_fileName)
            , isAbsPath(_isAbsPath)
        {
        }

        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("redirect_to", redirectFileName);
            json.Jsonize("is_absolute_path", isAbsPath, isAbsPath);
        }
        static bool isRedirectInfo(const std::string& str);

    public:
        std::string redirectFileName;
        bool isAbsPath;
    };

public:
    LocalLuaScriptReader();
    ~LocalLuaScriptReader();

private:
    LocalLuaScriptReader(const LocalLuaScriptReader&);
    LocalLuaScriptReader& operator=(const LocalLuaScriptReader&);

public:
    // download lua scripts from zk & etc/bs/lua
    static bool initLocalPath(const std::string& zkRoot);

    // get lua config file from env
    static bool getEnvLuaPathInfo(std::string& luaPath, ConfigInfo& configInfo);

    // get lua path in local work dir
    static std::string getLocalLuaPath(const std::string& luaPathId);

    // check lua path existance in local work dir
    static bool isLocalLuaPathExist(const std::string& luaPathId, bool& zip);

    static bool preloadZipLuaPath(const std::string& luaPathId);
    static bool isExist(const std::string& filePath);

    static bool readScriptFile(const TaskResourceManagerPtr& resMgr, const std::string& rootPath,
                               const std::string& fileName, std::string& actualFilePath, std::string& content);

    static std::string getRelativeScriptFileName(const std::string& filePath, const std::string& rootPath);

private:
    static bool downloadFromZkLuaRoot(const std::string& zkRoot);
    static bool syncEnvLuaPathToZk(const std::string& envLuaPath, const ConfigInfo& configInfo,
                                   const std::string& zkRoot);

    static bool copyLuaPath(const std::string& srcPath, const ConfigInfo& configInfo, const std::string& dstPath);

    static bool reuseLocalLuaPath();

    static std::string resetRootPathWithGraphNamespace(const std::string& rootPath, const std::string& nsPath);

    // get file path from multi root dir
    static std::string getScriptFilePath(const std::string& rootPath, const std::string& fileName);

    static bool readFile(const std::string& filePath, std::string& content);

    static bool doReadScriptFile(const config::ResourceReaderPtr& reader, const std::string& rootPath,
                                 const std::string& fileName, std::string& actualFilePath, std::string& content);

    bool isExistInZip(const std::string& filePath) const;
    bool readFileInZip(const std::string& filePath, std::string& content) const;
    bool loadZipLuaFile(const std::string& zipLuaFile);

private:
    std::unordered_map<std::string, std::string> _zipInnerFiles;
    std::unordered_set<std::string> _zipFileSet;
    size_t _totalInnerFileLen;
    mutable autil::ThreadMutex _lock;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(LocalLuaScriptReader);

}} // namespace build_service::admin
