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

namespace indexlibv2::framework {

struct ImportExternalFileOptions : public autil::legacy::Jsonizable {
    void Jsonize(JsonWrapper& json) override
    {
        json.Jsonize("move_files", moveFiles, moveFiles);
        json.Jsonize("failed_move_fall_back_to_copy", failedMoveFallBackToCopy, failedMoveFallBackToCopy);
        json.Jsonize("import_behind", importBehind, importBehind);
        json.Jsonize("fail_if_not_bottom_most_level", failIfNotBottomMostLevel, failIfNotBottomMostLevel);
    }
    bool moveFiles = false;
    bool failedMoveFallBackToCopy = true;
    bool importBehind = false;
    bool failIfNotBottomMostLevel = false;
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
