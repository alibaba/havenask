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
#ifndef ISEARCH_BS_ZIPFILEUTIL_H
#define ISEARCH_BS_ZIPFILEUTIL_H

#include <unordered_map>

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace util {

class ZipFileUtil
{
private:
    ZipFileUtil();
    ~ZipFileUtil();

public:
    /*
     * @param fileName: absolute path for zip file
     * load zip file to a map(innerFileName -> fileContent);
     */
    static bool readZipFile(const std::string& fileName, std::unordered_map<std::string, std::string>& result);

private:
    ZipFileUtil(const ZipFileUtil&);
    ZipFileUtil& operator=(const ZipFileUtil&);

public:
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ZipFileUtil);

}} // namespace build_service::util

#endif // ISEARCH_BS_ZIPFILEUTIL_H
