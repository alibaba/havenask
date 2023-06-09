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
#include "indexlib/analyzer/TextBuffer.h"

#include "autil/codec/EncodeConverter.h"

using namespace std;
namespace indexlibv2 { namespace analyzer {

AUTIL_LOG_SETUP(indexlib.analyzer, TextBuffer);

TextBuffer::TextBuffer()
{
    _size = 10240;
    _buf = new char[_size];
    _orignalUTF16Text = NULL;
    _normalizedUTF16Text = NULL;
    _normalizedUTF8Text = NULL;
    _orignalUTF16TextLen = 0;
    _curTokenPos = 0;
}

TextBuffer::~TextBuffer() { delete[] _buf; }

/*
 * strLen is the utf8 orignal string length, there are three temp buf
 * in a TextBuffer, the _normalizedUTF8Text is used to hold the normalized
 * utf8 string, its len is same as strLen; _orignalUTF16Text is used to
 * hold the utf16 text of orignal utf8 string, its max size is double of
 * strLen, _normalizedUTF8Text is used to hold the normalized utf16 text,
 * its length is same as _orignalUTF16Text. so, the total buf size is
 * 5 * strLen + 1.
 * TODO: the buf size is not aligned.
 */
void TextBuffer::resize(int32_t strLen)
{
    int32_t newSize = strLen * 5 + 1;
    if (newSize > _size) {
        delete[] _buf;
        _buf = new char[newSize];
        _size = newSize;
    }
    reset(strLen);
}

void TextBuffer::reset(int32_t strLen)
{
    _orignalUTF16Text = (uint16_t*)_buf;
    _normalizedUTF16Text = (uint16_t*)(_buf + 2 * strLen);
    _normalizedUTF8Text = _buf + 4 * strLen;
    _orignalUTF16TextLen = 0;
    _curTokenPos = 0;
}

void TextBuffer::normalize(const autil::codec::NormalizerPtr& normalizerPtr, const char* str, size_t& len)
{
    resize(len);
    _orignalUTF16TextLen = autil::codec::EncodeConverter::utf8ToUtf16(str, len, _orignalUTF16Text);
    normalizerPtr->normalizeUTF16(_orignalUTF16Text, _orignalUTF16TextLen, _normalizedUTF16Text);
    len = autil::codec::EncodeConverter::utf16ToUtf8(_normalizedUTF16Text, _orignalUTF16TextLen, _normalizedUTF8Text);
    _normalizedUTF8Text[len] = '\0';
    AUTIL_LOG(DEBUG, "normalized string: [%s]", _buf);
}

bool TextBuffer::nextTokenOrignalText(const std::string& tokenNormalizedUTF8Text, std::string& tokenOrignalText)
{
    int32_t utf16Len = autil::codec::EncodeConverter::utf8ToUtf16Len(tokenNormalizedUTF8Text.c_str(),
                                                                     tokenNormalizedUTF8Text.length());
    if (utf16Len < 0 || _curTokenPos + utf16Len > _orignalUTF16TextLen) {
        AUTIL_LOG(DEBUG, "failed to calc the orignal text len, use the normalized text");
        return false;
    } else {
        // reuse the normalized utf16 text buffer
        char* buf = (char*)_normalizedUTF16Text;
        int32_t utf8Len = autil::codec::EncodeConverter::utf16ToUtf8(_orignalUTF16Text + _curTokenPos, utf16Len, buf);
        tokenOrignalText.assign(buf, utf8Len);
        _curTokenPos += utf16Len;
    }
    return true;
}

}} // namespace indexlibv2::analyzer
