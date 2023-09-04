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

namespace swift {
namespace util {

class TargetRecorder {
public:
    TargetRecorder();
    ~TargetRecorder();

public:
    static void newAdminTopic(const std::string &target);
    static void newAdminPartition(const std::string &target);
    static void setMaxFileCount(size_t count);
    static bool getCurrentPath(std::string &path);
    static void newRecord(const std::string &content, const std::string &dir, const std::string &suffix);

private:
    static bool hasSuffix(const std::string &str, const std::string &suffix);
    static size_t _maxCount;
};

} // namespace util
} // namespace swift
