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
#include <memory>

#include "autil/Log.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlib::config {
class IndexPartitionSchema;
}

namespace indexlib::legacy::index_base {

class SchemaAdapter
{
public:
    static void LoadAdaptiveBitmapTerms(const std::shared_ptr<file_system::Directory>& rootDir,
                                        config::IndexPartitionSchema* schema);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::legacy::index_base
