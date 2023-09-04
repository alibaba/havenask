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
#ifndef CARBON_LZMACOMPRESS_H
#define CARBON_LZMACOMPRESS_H

#include "common/common.h"
#include "carbon/Log.h"
BEGIN_CARBON_NAMESPACE(common);

class LzmaCompress
{
public:
    LzmaCompress(uint32_t preset = 1);
    ~LzmaCompress();
    bool compress(const std::string &input, std::string &output);
    bool decompress(const std::string &input, std::string &output);
private:
    LzmaCompress(const LzmaCompress &);
    LzmaCompress& operator=(const LzmaCompress &);
private:
    uint32_t _preset;
};

CARBON_TYPEDEF_PTR(LzmaCompress);

END_CARBON_NAMESPACE(common);

#endif //CARBON_LZMACOMPRESS_H
