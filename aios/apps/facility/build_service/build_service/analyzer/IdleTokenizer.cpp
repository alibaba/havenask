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
#include "build_service/analyzer/IdleTokenizer.h"

namespace build_service { namespace analyzer {
BS_LOG_SETUP(analyzer, IdleTokenizer);

IdleTokenizer::IdleTokenizer()
{
    _firstToken = true;
    _text = NULL;
}

IdleTokenizer::~IdleTokenizer() {}

void IdleTokenizer::tokenize(const char* text, size_t len)
{
    _text = text;
    _len = len;
    _firstToken = true;
}

bool IdleTokenizer::next(Token& token)
{
    if (!_firstToken || _len == 0) {
        return false;
    }

    token.getNormalizedText().assign(_text, _text + _len);
    token.setPosition(0);
    token.setIsRetrieve(true);
    _firstToken = false;
    return true;
}

indexlibv2::analyzer::ITokenizer* IdleTokenizer::clone() { return new IdleTokenizer(); }

}} // namespace build_service::analyzer
