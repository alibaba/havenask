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
#ifndef ISEARCH_BS_PREFIXTOKENIZERIMPL_H
#define ISEARCH_BS_PREFIXTOKENIZERIMPL_H

#include "build_service/analyzer/Token.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace analyzer {

class PrefixTokenizerImpl
{
public:
    PrefixTokenizerImpl(uint32_t maxTokenNum, bool includeSelf);
    ~PrefixTokenizerImpl();

private:
    PrefixTokenizerImpl(const PrefixTokenizerImpl&);
    PrefixTokenizerImpl& operator=(const PrefixTokenizerImpl&);

public:
    bool tokenize(const char* text, size_t len);
    bool next(Token& token);
    void reset();

private:
    uint32_t _maxTokenNum;
    bool _includeSelf;
    std::vector<size_t> _posVec;
    const char* _text;
    size_t _textLen;
    size_t _position;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(PrefixTokenizerImpl);

}} // namespace build_service::analyzer

#endif // ISEARCH_BS_PREFIXTOKENIZERIMPL_H
