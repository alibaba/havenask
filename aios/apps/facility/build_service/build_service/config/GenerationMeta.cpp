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
#include "build_service/config/GenerationMeta.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/util/ErrorLogCollector.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace autil::legacy;

using namespace build_service::util;

namespace build_service { namespace config {

BS_LOG_SETUP(config, GenerationMeta);

const string GenerationMeta::FILE_NAME = "generation_meta";

GenerationMeta::GenerationMeta() {}

GenerationMeta::~GenerationMeta() {}

bool GenerationMeta::loadFromFile(const string& indexPartitionDir)
{
    string fileName = fslib::util::FileUtil::joinFilePath(indexPartitionDir, FILE_NAME);
    if (fslib::EC_FALSE == fslib::fs::FileSystem::isExist(fileName)) {
        string generationDir = fslib::util::FileUtil::getParentDir(indexPartitionDir);
        fileName = fslib::util::FileUtil::joinFilePath(generationDir, FILE_NAME);
        if (fslib::EC_FALSE == fslib::fs::FileSystem::isExist(fileName)) {
            return true;
        }
    }
    string context;
    if (!fslib::util::FileUtil::readFile(fileName, context)) {
        string errorMsg = "load generation meta[" + fileName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    try {
        FromJsonString(_meta, context);
    } catch (const autil::legacy::ExceptionBase& e) {
        string errorMsg = "parse generation meta[" + context + "] failed.";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool GenerationMeta::operator==(const GenerationMeta& other) const { return _meta == other._meta; }

}} // namespace build_service::config
