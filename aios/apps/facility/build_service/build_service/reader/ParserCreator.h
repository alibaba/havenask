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
#ifndef ISEARCH_BS_PARSERCREATOR_H
#define ISEARCH_BS_PARSERCREATOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "indexlib/document/document_factory_wrapper.h"
#include "indexlib/document/raw_document_parser.h"

BS_DECLARE_REFERENCE_CLASS(proto, ParserConfig);

namespace build_service { namespace reader {

class HologresInterface;

class ParserCreator
{
public:
    ParserCreator(
        const indexlib::document::DocumentFactoryWrapperPtr& wrapper = indexlib::document::DocumentFactoryWrapperPtr(),
        std::shared_ptr<HologresInterface> hologresInterface = nullptr)
        : _wrapper(wrapper)
        , _hologresInterface(hologresInterface)
    {
    }
    ~ParserCreator() {}

private:
    ParserCreator(const ParserCreator&);
    ParserCreator& operator=(const ParserCreator&);

public:
    bool createParserConfigs(const KeyValueMap& kvMap, std::vector<proto::ParserConfig>& parserConfigs);
    indexlib::document::RawDocumentParser* create(const KeyValueMap& kvMap);
    const std::string& getLastError() const { return _lastErrorStr; }
    indexlib::document::RawDocumentParser* createSingleParser(const proto::ParserConfig& parserConfig);

private:
    indexlib::document::RawDocumentParser* doCreateParser(const std::vector<proto::ParserConfig>& parserConfigs);
    bool createParserConfigForLegacyConfig(const KeyValueMap& kvMap, proto::ParserConfig& parserConfig);

private:
    bool _supportIndexParser = true;
    std::string _lastErrorStr;
    indexlib::document::DocumentFactoryWrapperPtr _wrapper;
    std::shared_ptr<HologresInterface> _hologresInterface = nullptr;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ParserCreator);

}} // namespace build_service::reader

#endif // ISEARCH_BS_PARSERCREATOR_H
