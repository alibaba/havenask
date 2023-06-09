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
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace indexlib { namespace file_system {

class InnerFileMeta : public autil::legacy::Jsonizable
{
public:
    InnerFileMeta(const std::string& path = "", bool isDir = false);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Init(size_t offset, size_t length, uint32_t fileIdx);
    const std::string& GetFilePath() const { return _filePath; }
    size_t GetOffset() const { return _offset; }
    size_t GetLength() const { return _length; }
    bool IsDir() const { return _isDir; }
    uint32_t GetDataFileIdx() const { return _fileIdx; }
    void SetDataFileIdx(uint32_t fileIdx) { _fileIdx = fileIdx; }
    void SetFilePath(const std::string& filePath) { _filePath = filePath; }
    bool operator==(const InnerFileMeta& other) const;
    bool operator<(const InnerFileMeta& other) const { return _filePath < other._filePath; }
    std::string DebugString() const;

public:
    typedef std::vector<InnerFileMeta> InnerFileMetaVec;
    typedef InnerFileMetaVec::const_iterator Iterator;

private:
    std::string _filePath;
    size_t _offset;
    size_t _length;
    uint32_t _fileIdx;
    bool _isDir;

private:
    AUTIL_LOG_DECLARE();
};
}} // namespace indexlib::file_system
