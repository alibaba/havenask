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

#include "autil/Log.h"
#include "indexlib/util/buffer_compressor/CompressHintData.h"

namespace indexlib::util {

class Lz4CompressHintData : public CompressHintData
{
public:
    Lz4CompressHintData() {}
    ~Lz4CompressHintData() {}

    Lz4CompressHintData(const Lz4CompressHintData&) = delete;
    Lz4CompressHintData& operator=(const Lz4CompressHintData&) = delete;
    Lz4CompressHintData(Lz4CompressHintData&&) = delete;
    Lz4CompressHintData& operator=(Lz4CompressHintData&&) = delete;

public:
    bool Init(const autil::StringView& data, bool needCopy) override
    {
        if (needCopy) {
            _data = std::string(data.data(), data.size());
            _dataRef = autil::StringView(_data);
        } else {
            _dataRef = data;
        }
        _needCopy = needCopy;
        return true;
    }

    const autil::StringView& GetData() const { return _dataRef; }

private:
    autil::StringView _dataRef;
    std::string _data;
    bool _needCopy = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::util
