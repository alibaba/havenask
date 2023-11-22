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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

struct ImportExternalFileOptions : public autil::legacy::Jsonizable {
    static constexpr const char* INDEX_MODE = "index_mode";
    static constexpr const char* VERSION_MODE = "version_mode";

    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("move_files", moveFiles, moveFiles);
        json.Jsonize("failed_move_fall_back_to_copy", failedMoveFallBackToCopy, failedMoveFallBackToCopy);
        json.Jsonize("import_behind", importBehind, importBehind);
        json.Jsonize("fail_if_not_bottom_most_level", failIfNotBottomMostLevel, failIfNotBottomMostLevel);
        json.Jsonize("ignore_duplicate_files", ignoreDuplicateFiles, ignoreDuplicateFiles);
        json.Jsonize("mode", mode, mode);
    }
    bool IsValidMode() const
    {
        if (mode != INDEX_MODE && mode != VERSION_MODE) {
            return false;
        }
        return true;
    }

    bool moveFiles = false;
    bool failedMoveFallBackToCopy = true;
    bool importBehind = false;
    bool failIfNotBottomMostLevel = false;
    bool ignoreDuplicateFiles = false;
    std::string mode = INDEX_MODE;
};

class ImportOptions
{
public:
    ImportOptions() = default;
    ~ImportOptions() = default;

    const std::string& GetImportStrategy() const { return _importStrategy; }
    void SetImportStrategy(const std::string& strategy) { _importStrategy = strategy; }
    const std::string& GetImportType() const { return _importType; }
    void SetImportType(const std::string& type) { _importType = type; }

private:
    std::string _importStrategy;
    std::string _importType;
};

} // namespace indexlibv2::framework
