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
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h"
#include "autil/codec/CodeConverter.h"
#include "autil/codec/NormalizeOptions.h"
#include "autil/codec/NormalizeTable.h"

namespace autil {
namespace codec {

class Normalizer
{
public:
    enum {
        K_OP_NORMALIZE = 0x01,  // 需要执行normalize操作，主要用于编码转换时
    };
public:
    Normalizer(const NormalizeOptions &options = NormalizeOptions(),
            const std::map<uint16_t, uint16_t> *traditionalTablePatch = NULL);
    ~Normalizer();
public:
    void normalize(const std::string &word, std::string &normalizeWord);
    void normalizeUTF16(const uint16_t *in, size_t len,
                        uint16_t *out);
    bool needNormalize() {
        return !(_options.traditionalSensitive &&
                 _options.widthSensitive &&
                 _options.caseSensitive);
    }

public:
   bool gbkToUtf8(const std::string &input, std::string &output, unsigned op = 0) const;

   bool utf8ToGbk(const std::string &input, std::string &output, unsigned op = 0) const;

   bool strNormalizeUtf8(const std::string &input, std::string &output) const;

   bool strNormalizeGbk(const std::string &input, std::string &output) const;

   bool unicodeToUtf8(const std::string &input, std::string &output) const;

private:
   typedef int (*TO_UNICODE_FUN)(U8CHAR *, unsigned int, U16CHAR *, unsigned int);
   typedef int (*FROM_UNICODE_FUN)(U16CHAR *, unsigned int, U8CHAR *, unsigned int);
   bool stringConverter(const std::string &input, std::string &output, TO_UNICODE_FUN to_unicode,
                        FROM_UNICODE_FUN from_unicode, unsigned op = 0) const;

private:
    NormalizeOptions _options;
    NormalizeTable _table;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<Normalizer> NormalizerPtr;

}
}

