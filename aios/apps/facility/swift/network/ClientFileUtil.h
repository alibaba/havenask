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

#include "autil/Log.h"
#include "swift/common/Common.h"

namespace swift {
namespace network {

class ClientFileUtil {
public:
    ClientFileUtil();
    ~ClientFileUtil();

private:
    ClientFileUtil(const ClientFileUtil &);
    ClientFileUtil &operator=(const ClientFileUtil &);

public:
    bool readFile(const std::string &fileName, std::string &content);

private:
    bool readLocalFile(const std::string &fileName, std::string &content);
    bool readZfsFile(const std::string &fileName, std::string &content);
    bool readHttpFile(const std::string &fileName, std::string &content);
    std::string getFsType(const std::string &srcPath);
    bool parsePath(const std::string &type, const std::string &fileName, std::string &server, std::string &path);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(ClientFileUtil);

} // namespace network
} // namespace swift
