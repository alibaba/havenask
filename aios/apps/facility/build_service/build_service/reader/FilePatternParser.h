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
#ifndef ISEARCH_BS_FILEPATTERNPARSER_H
#define ISEARCH_BS_FILEPATTERNPARSER_H

#include "build_service/common_define.h"
#include "build_service/reader/FilePattern.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class FilePatternParser
{
private:
    FilePatternParser();
    ~FilePatternParser();
    FilePatternParser(const FilePatternParser&);
    FilePatternParser& operator=(const FilePatternParser&);

public:
    static FilePatterns parse(const std::string& filePatternStr, const std::string& rootDir);

private:
    static void normalizeFilePatterns(std::vector<std::string>& filePatterns);
    static void dedupFilePatterns(std::vector<std::string>& filePatterns);
    static FilePattern* parseFilePattern(const std::string& patternStr);

private:
    static const std::string FILE_PATTERN_PREFIX_SEP;
    static const std::string FILE_PATTERN_SURFIX_SEP;
    static const std::string FILE_PATTERN_SEP;
    static const std::string FILE_PATTERN_PATH_SEP;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(FilePatternParser);

}} // namespace build_service::reader

#endif // ISEARCH_BS_FILEPATTERNPARSER_H
