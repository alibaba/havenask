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
#include "autil/codec/Normalizer.h"

namespace indexlibv2 { namespace analyzer {

/*
 * TextBuffer is used to optimize the memory new/delete when the
 * filed text is tokenized. And this buffer is reused.
 */
class TextBuffer
{
public:
    TextBuffer();
    ~TextBuffer();

public:
    /*
     * normalize the str(utf8) to _normalizedUTF8Text, and trans the str
     * to utf16 coped into _orignalUTF16Text. the _normalizedUTF16Text is
     * temp buf when encode converting, and it can be reuse in future.
     */
    void normalize(const autil::codec::NormalizerPtr& normalizerPtr, const char* str, size_t& len);
    bool nextTokenOrignalText(const std::string& tokenNormalizedUTF8Text, std::string& tokenOrignalText);
    char* getNormalizedUTF8Text() { return _normalizedUTF8Text; }

private:
    void resize(int32_t strLen);
    void reset(int32_t strLen);

private:
    char* _buf;
    uint16_t* _orignalUTF16Text;
    uint16_t* _normalizedUTF16Text;
    char* _normalizedUTF8Text;
    int32_t _size;
    int32_t _orignalUTF16TextLen;
    int32_t _curTokenPos;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::analyzer
