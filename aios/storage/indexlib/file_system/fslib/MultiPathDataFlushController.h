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

#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/file_system/fslib/DataFlushController.h"
#include "indexlib/util/Singleton.h"

namespace indexlib { namespace file_system {

class MultiPathDataFlushController : public util::Singleton<MultiPathDataFlushController>
{
public:
    MultiPathDataFlushController();
    ~MultiPathDataFlushController();

public:
    static DataFlushController* GetDataFlushController(const std::string& filePath) noexcept;

public:
    DataFlushController* FindDataFlushController(const std::string& filePath) const noexcept;
    void InitFromString(const std::string& initStr);
    void Clear();

private:
    void InitFromEnv();

private:
    std::vector<DataFlushControllerPtr> _dataFlushControllers;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MultiPathDataFlushController> MultiPathDataFlushControllerPtr;
}} // namespace indexlib::file_system
