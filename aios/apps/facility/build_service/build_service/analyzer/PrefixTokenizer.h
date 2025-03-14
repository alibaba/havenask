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

#include "build_service/analyzer/PrefixTokenizerImpl.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/common_define.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/util/Log.h"
#include "indexlib/analyzer/ITokenizer.h"
#include "indexlib/base/Constant.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace analyzer {

class PrefixTokenizer : public Tokenizer
{
public:
    PrefixTokenizer();
    PrefixTokenizer(uint32_t maxTokenNum, bool includeSelf,
                    const std::string& seperator = std::string(1, MULTI_VALUE_SEPARATOR));
    ~PrefixTokenizer();

private:
    PrefixTokenizer& operator=(const PrefixTokenizer&);

public:
    bool init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReader) override;
    void tokenize(const char* text, size_t len) override;
    bool next(Token& token) override;
    indexlibv2::analyzer::ITokenizer* clone() override;

private:
    void clear();

private:
    uint32_t _maxTokenNum;
    bool _includeSelf;
    std::string _seperator;

    const char* _text;
    size_t _position;
    std::vector<PrefixTokenizerImpl*> _impls;
    size_t _cursor;

private:
    static const uint32_t DEFAULT_MAX_TOKEN_NUM;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrefixTokenizer);

}} // namespace build_service::analyzer
