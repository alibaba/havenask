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

namespace indexlibv2 { namespace analyzer {

enum TokenRetrieveLevel { TRL_INVALID, TRL_RETRIEVE, TRL_SEMANTIC };

static const std::string ANALYZER_CONFIG_FILE = "analyzer.json";

static const std::string TOKENIZER_TYPE = "tokenizer_type";
static const std::string TOKENIZER_ID = "tokenizer_id";
static const std::string TOKEN_RETRIEVE_LEVEL = "token_retrieve_level";
static const std::string DELIMITER = "delimiter";
static const std::string ALIWS_ANALYZER = "aliws";
static const std::string MULTILEVEL_ALIWS_ANALYZER = "multilevel_aliws";
static const std::string SEMANTIC_ALIWS_ANALYZER = "semantic_aliws";
static const std::string SIMPLE_ANALYZER = "simple";
static const std::string SINGLEWS_ANALYZER = "singlews";
static const std::string PREFIX_ANALYZER = "prefix";
static const std::string SUFFIX_PREFIX_ANALYZER = "suffix_prefix";

static const std::string TOKEN_RETRIEVE_LEVEL_RETRIEVE = "retrieve";
static const std::string TOKEN_RETRIEVE_LEVEL_SEMANTIC = "semantic";

static const std::string ANALYZER_NAME = "analyzer_name";
static const std::string TOKENIZER_NAME = "tokenizer_name";
static const std::string TOKENIZER_CONFIGS = "tokenizer_configs";
static const std::string STOPWORDS = "stopwords";
static const std::string NORMALIZE_OPTIONS = "normalize_options";

static const std::string USE_EXTEND_RETRIEVE = "use_extend_retrieve";
static const std::string USE_MAX_SEMANTIC = "use_max_semantic";

static const std::string ALITOKENIZER_CONF_FILE = "AliTokenizer.conf";

}} // namespace indexlibv2::analyzer
