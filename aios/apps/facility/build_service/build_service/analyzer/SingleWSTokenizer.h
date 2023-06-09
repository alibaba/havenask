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
#ifndef ISEARCH_BS_SINGLEWSTOKENIZER_H
#define ISEARCH_BS_SINGLEWSTOKENIZER_H

#include "build_service/analyzer/SingleWSScanner.h"
#include "build_service/analyzer/Tokenizer.h"
#include "build_service/util/Log.h"

namespace indexlibv2::analyzer {
class ITokenizer;
}

namespace build_service { namespace analyzer {

class SingleWSTokenizer : public Tokenizer
{
public:
    SingleWSTokenizer();
    ~SingleWSTokenizer();

public:
    bool init(const KeyValueMap& parameters, const config::ResourceReaderPtr& resourceReaderPtr) override
    {
        return true;
    }
    void tokenize(const char* text, size_t len) override;
    bool next(Token& token) override;
    indexlibv2::analyzer::ITokenizer* clone() override;

private:
    void clear();

private:
    SingleWSScanner* _scanner;
    std::istringstream* _iss;
    const char* _pToken;
    int _tokenLen;
    uint32_t _position;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleWSTokenizer);

}} // namespace build_service::analyzer

#endif // ISEARCH_BS_SINGLEWSTOKENIZER_H
