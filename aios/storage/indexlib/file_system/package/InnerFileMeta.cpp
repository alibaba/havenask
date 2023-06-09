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
#include "indexlib/file_system/package/InnerFileMeta.h"

#include <cstdint>
#include <ostream>

using std::endl;
using std::string;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, InnerFileMeta);

InnerFileMeta::InnerFileMeta(const string& path, bool isDir)
    : _filePath(path)
    , _offset(0)
    , _length(0)
    , _fileIdx(0)
    , _isDir(isDir)
{
}

void InnerFileMeta::Init(size_t offset, size_t length, uint32_t fileIdx)
{
    _offset = offset;
    _length = length;
    _fileIdx = fileIdx;
}

bool InnerFileMeta::operator==(const InnerFileMeta& other) const
{
    return _filePath == other._filePath && _offset == other._offset && _length == other._length &&
           _fileIdx == other._fileIdx && _isDir == other._isDir;
}

void InnerFileMeta::Jsonize(JsonWrapper& json)
{
    json.Jsonize("path", _filePath);
    json.Jsonize("offset", _offset);
    json.Jsonize("length", _length);
    json.Jsonize("fileIdx", _fileIdx);
    json.Jsonize("isDir", _isDir);
}

string InnerFileMeta::DebugString() const
{
    std::stringstream ss;
    ss << "Path: " << _filePath << " Offset: " << _offset << " Length: " << _length << " FileIdx: " << _fileIdx
       << "IsDir: " << _isDir << endl;
    return ss.str();
}
}} // namespace indexlib::file_system
