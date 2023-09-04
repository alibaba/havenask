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
#include "build_service/reader/ParserCreator.h"

#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/reader/CombinedRawDocumentParser.h"
#include "build_service/reader/DocumentSeparators.h"
#include "build_service/reader/IdleDocumentParser.h"
#include "build_service/reader/StandardRawDocumentParser.h"
#include "build_service/reader/SwiftFieldFilterRawDocumentParser.h"
#include "build_service/reader/SwiftSchemaBasedRawDocumentParser.h"


using namespace std;

BS_NAMESPACE_USE(proto);
BS_NAMESPACE_USE(config);

namespace build_service { namespace reader {
BS_LOG_SETUP(reader, ParserCreator);

bool ParserCreator::createParserConfigs(const KeyValueMap& kvMap, vector<ParserConfig>& parserConfigs)
{
    string dsJsonStr = getValueFromKeyValueMap(kvMap, DATA_DESCRIPTION_KEY);
    if (dsJsonStr.empty()) {
        ParserConfig parserConfig;
        if (!createParserConfigForLegacyConfig(kvMap, parserConfig)) {
            return false;
        }
        parserConfigs.push_back(parserConfig);
        return true;
    }

    DataDescription ds;
    try {
        autil::legacy::FromJsonString(ds, dsJsonStr);
    } catch (const autil::legacy::ExceptionBase& e) {
        _lastErrorStr = "Invalid json string for dataDescription[" + dsJsonStr + "] : error [" + string(e.what()) + "]";
        return false;
    }
    if (ds.getParserConfigCount() == 0 || !_supportIndexParser) {
        ParserConfig parserConfig;
        if (!createParserConfigForLegacyConfig(kvMap, parserConfig)) {
            return false;
        }
        parserConfigs.push_back(parserConfig);
        return true;
    }
    parserConfigs = ds.parserConfigs;
    return true;
}

RawDocumentParser* ParserCreator::create(const KeyValueMap& kvMap)
{
    _lastErrorStr = "";
    // NOTE
    // BS_SUPPORT_INDEX_PARSER是临时方案，为了解决v2不支持index_parser的问题。v2实现index_parser时应该删除本commit。
    _supportIndexParser = autil::EnvUtil::getEnv("BS_SUPPORT_INDEX_PARSER", true);
    vector<ParserConfig> parserConfigs;
    if (!createParserConfigs(kvMap, parserConfigs)) {
        return NULL;
    }
    return doCreateParser(parserConfigs);
}

bool ParserCreator::createParserConfigForLegacyConfig(const KeyValueMap& kvMap, ParserConfig& parserConfig)
{
    string sourceType = getValueFromKeyValueMap(kvMap, READ_SRC_TYPE);
    string needFieldFilter = getValueFromKeyValueMap(kvMap, NEED_FIELD_FILTER, "false");
    string useSchema = getValueFromKeyValueMap(kvMap, USE_FIELD_SCHEMA, "false");
    if (sourceType == SWIFT_READ_SRC) {
        if (needFieldFilter == "true" && useSchema == "true") {
            _lastErrorStr = "createRawDocumentParserFailed: "
                            "not support needFieldFilter & useFieldSchema both true";
            return false;
        }
        if (needFieldFilter == "true") {
            parserConfig.type = RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER;
            return true;
        } else if (needFieldFilter != "false") {
            _lastErrorStr = "createRawDocumentParserFailed: "
                            "unknown needFieldFilter option :" +
                            needFieldFilter;
            return false;
        }
        if (useSchema == "true") {
            parserConfig.type = RAW_DOCUMENT_FORMAT_SWIFT_USE_SCHEMA;
            return true;
        } else if (useSchema != "false") {
            _lastErrorStr = "createRawDocumentParserFailed: "
                            "unknown useFieldSchema option :" +
                            useSchema;
            return false;
        }
    }

    string format = getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_FORMAT);
    if (format.empty()) {
        parserConfig.type = RAW_DOCUMENT_FORMAT_CUSTOMIZED;
        parserConfig.parameters[RAW_DOCUMENT_FIELD_SEP] =
            getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_FIELD_SEP, RAW_DOCUMENT_HA3_FIELD_SEP);
        parserConfig.parameters[RAW_DOCUMENT_KV_SEP] =
            getValueFromKeyValueMap(kvMap, RAW_DOCUMENT_KV_SEP, RAW_DOCUMENT_HA3_KV_SEP);
        return true;
    }

    if (format == RAW_DOCUMENT_FORMAT_SELF_EXPLAIN ||
        (format == RAW_DOCUMENT_FORMAT_INDEXLIB_PARSER && !_supportIndexParser)) {
        string fieldName = getValueFromKeyValueMap(kvMap, DOC_STRING_FIELD_NAME);
        if (fieldName == "") {
            fieldName = DOC_STRING_FIELD_NAME_DEFAULT;
        }
        parserConfig.type = RAW_DOCUMENT_FORMAT_SELF_EXPLAIN;
        parserConfig.parameters[DOC_STRING_FIELD_NAME] = fieldName;
        return true;
    }

    parserConfig.type = format;
    return true;
}

RawDocumentParser* ParserCreator::doCreateParser(const vector<ParserConfig>& parserConfigs)
{
    assert(parserConfigs.size() > 0);
    if (parserConfigs.size() == 1) {
        return createSingleParser(parserConfigs[0]);
    }
    vector<RawDocumentParserPtr> parsers;
    for (const auto& parserConfig : parserConfigs) {
        RawDocumentParserPtr parser(createSingleParser(parserConfig));
        if (!parser) {
            return NULL;
        }
        parsers.push_back(parser);
    }
    return new CombinedRawDocumentParser(parsers);
}

RawDocumentParser* ParserCreator::createSingleParser(const ParserConfig& parserConfig)
{
    if (parserConfig.type == RAW_DOCUMENT_HA3_DOCUMENT_FORMAT) {
        return new StandardRawDocumentParser(RAW_DOCUMENT_HA3_FIELD_SEP, RAW_DOCUMENT_HA3_KV_SEP);
    }
    if (parserConfig.type == RAW_DOCUMENT_ISEARCH_DOCUMENT_FORMAT) {
        return new StandardRawDocumentParser(RAW_DOCUMENT_ISEARCH_FIELD_SEP, RAW_DOCUMENT_ISEARCH_KV_SEP);
    }
    if (parserConfig.type == RAW_DOCUMENT_FORMAT_SELF_EXPLAIN) {
        string fieldName = getValueFromKeyValueMap(parserConfig.parameters, DOC_STRING_FIELD_NAME);
        if (fieldName == "") {
            fieldName = DOC_STRING_FIELD_NAME_DEFAULT;
        }
        return new IdleDocumentParser(fieldName);
    }
    if (parserConfig.type == RAW_DOCUMENT_FORMAT_CUSTOMIZED) {
        string fieldSep = getValueFromKeyValueMap(parserConfig.parameters, RAW_DOCUMENT_FIELD_SEP);
        string keyValueSep = getValueFromKeyValueMap(parserConfig.parameters, RAW_DOCUMENT_KV_SEP);
        if (fieldSep.empty() || keyValueSep.empty()) {
            _lastErrorStr = "createRawDocumentParser failed: "
                            "invalid seperator for customized parser, fieldSep: " +
                            fieldSep + ", keyValueSep: " + keyValueSep + " can't be empty";
            return NULL;
        }
        return new StandardRawDocumentParser(fieldSep, keyValueSep);
    }
    if (parserConfig.type == RAW_DOCUMENT_FORMAT_SWIFT_FILED_FILTER) {
        return new SwiftFieldFilterRawDocumentParser();
    }

    if (parserConfig.type == RAW_DOCUMENT_FORMAT_SWIFT_USE_SCHEMA) {
        return new SwiftSchemaBasedRawDocumentParser();
    }


    if (parserConfig.type == RAW_DOCUMENT_FORMAT_INDEXLIB_PARSER) {
        RawDocumentParser* parser = nullptr;
        if (_wrapper) {
            parser = _wrapper->CreateRawDocumentParser(parserConfig.parameters);
        }
        if (!parser) {
            _lastErrorStr = "createRawDocumentParser failed: "
                            "create RawDocumentParser from documentFactoryWrapper fail!";
        }
        return parser;
    }

    _lastErrorStr = "createRawDocumentParser failed: "
                    "unsupported parserConfig.type [" +
                    parserConfig.type + "]";
    return NULL;
}

}} // namespace build_service::reader
