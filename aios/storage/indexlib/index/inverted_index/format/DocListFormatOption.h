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
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {

class DocListFormatOption
{
public:
    explicit inline DocListFormatOption(optionflag_t optionFlag = OPTION_FLAG_ALL) { Init(optionFlag); }
    ~DocListFormatOption() = default;

    inline void Init(optionflag_t optionFlag)
    {
        _hasDocPayload = optionFlag & of_doc_payload ? 1 : 0;
        _hasFieldMap = optionFlag & of_fieldmap ? 1 : 0;
        if (optionFlag & of_term_frequency) {
            _hasTf = 1;
            if (optionFlag & of_tf_bitmap) {
                _hasTfBitmap = 1;
                _hasTfList = 0;
            } else {
                _hasTfBitmap = 0;
                _hasTfList = 1;
            }
        } else {
            _hasTf = 0;
            _hasTfList = 0;
            _hasTfBitmap = 0;
        }
        _shortListVbyteCompress = 0;
        _unused = 0;
    }

    bool HasTermFrequency() const { return _hasTf == 1; }
    bool HasTfList() const { return _hasTfList == 1; }
    bool HasTfBitmap() const { return _hasTfBitmap == 1; }
    bool HasDocPayload() const { return _hasDocPayload == 1; }
    bool HasFieldMap() const { return _hasFieldMap == 1; }
    bool operator==(const DocListFormatOption& right) const;
    bool IsShortListVbyteCompress() const { return _shortListVbyteCompress == 1; }
    void SetShortListVbyteCompress(bool flag) { _shortListVbyteCompress = flag ? 1 : 0; }

private:
    uint8_t _hasTf                  : 1;
    uint8_t _hasTfList              : 1;
    uint8_t _hasTfBitmap            : 1;
    uint8_t _hasDocPayload          : 1;
    uint8_t _hasFieldMap            : 1;
    uint8_t _shortListVbyteCompress : 1;
    uint8_t _unused                 : 2;

    friend class DocListEncoderTest;
    friend class DocListMemoryBufferTest;
    friend class JsonizableDocListFormatOption;

    AUTIL_LOG_DECLARE();
};

class JsonizableDocListFormatOption : public autil::legacy::Jsonizable
{
public:
    explicit JsonizableDocListFormatOption(DocListFormatOption option = DocListFormatOption())
        : _docListFormatOption(option)
    {
    }
    const DocListFormatOption& GetDocListFormatOption() const { return _docListFormatOption; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    DocListFormatOption _docListFormatOption;
};

} // namespace indexlib::index
