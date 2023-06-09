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
#include "indexlib/file_system/archive/LineReader.h"

#include <cstddef>
#include <memory>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LineReader);

LineReader::LineReader() : _buf(NULL), _size(0), _cursor(0) { _buf = new char[DEFAULT_BUFFER_SIZE]; }

LineReader::~LineReader()
{
    if (_buf) {
        delete[] _buf;
        _buf = NULL;
    }

    if (_fileReader) {
        auto ret = _fileReader->Close();
        if (!ret.OK()) {
            AUTIL_LOG(ERROR, "close file reader failed, ec[%d]", ret.Code());
        }
        _fileReader.reset();
    }
}

void LineReader::Open(const std::shared_ptr<FileReader>& fileReader) noexcept { _fileReader = fileReader; }
bool LineReader::NextLine(string& line, ErrorCode& ec) noexcept
{
    ec = FSEC_OK;
    if (!_fileReader) {
        return false;
    }
    line.clear();
    while (true) {
        if (_cursor == _size) {
            _cursor = 0;
            std::tie(ec, _size) = _fileReader->Read(_buf, DEFAULT_BUFFER_SIZE).CodeWith();
            if (unlikely(ec != FSEC_OK)) {
                return false;
            }
            if (_size == 0) {
                // asserte(_file->IsEof());
                return !line.empty();
            }
        }
        size_t pos = _cursor;
        for (; pos < _size && _buf[pos] != '\n'; pos++)
            ;
        if (pos < _size) {
            line.append(_buf + _cursor, pos - _cursor);
            _cursor = pos + 1;
            break;
        } else {
            line.append(_buf + _cursor, pos - _cursor);
            _cursor = pos;
        }
    }
    return true;
}
}} // namespace indexlib::file_system
