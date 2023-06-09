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

namespace indexlibv2::framework {

class ImportOptions
{
public:
    ImportOptions() {};
    ~ImportOptions() {};

public:
    enum DiffSrcImportStrategy { KEEP_SEGMENT_OVERWRITE_LOCATOR, KEEP_SEGMENT_IGNORE_LOCATOR, NOT_SUPPORT };
    DiffSrcImportStrategy GetImportStrategy() const { return _importStrategy; }
    void SetImportStrategy(DiffSrcImportStrategy strategy) { _importStrategy = strategy; }

private:
    DiffSrcImportStrategy _importStrategy = NOT_SUPPORT;
};

} // namespace indexlibv2::framework
