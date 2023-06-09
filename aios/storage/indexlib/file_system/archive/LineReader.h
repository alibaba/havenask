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
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib { namespace file_system {

class LineReader
{
public:
    LineReader();
    ~LineReader();

public:
    void Open(const std::shared_ptr<FileReader>& fileReader) noexcept;
    bool NextLine(std::string& line, ErrorCode& ec) noexcept;

private:
    std::shared_ptr<FileReader> _fileReader;
    char* _buf;
    size_t _size;
    size_t _cursor;

private:
    static const uint32_t DEFAULT_BUFFER_SIZE = 4096;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LineReader> LineReaderPtr;
}} // namespace indexlib::file_system
