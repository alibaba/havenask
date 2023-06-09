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


namespace isearch {
namespace common {

constexpr char CLAUSE_SEPERATOR[] = "&&";
constexpr char CLAUSE_KV_SEPERATOR[] = "=";
constexpr char CONFIG_KV_PAIR_SEPERATOR[] = ",";
constexpr char CONFIG_KV_SEPERATOR[] = ":";
constexpr char CONFIG_EXTRA_KV_PAIR_SEPERATOR[] = ";";
constexpr char CONFIG_EXTRA_KV_SEPERATOR[] = "#";
constexpr char CLAUSE_MULTIVALUE_SEPERATOR[] = "&";
constexpr char CLAUSE_SUB_MULTIVALUE_SEPERATOR[] = "|";
constexpr char KVPAIR_CLAUSE_KV_PAIR_SEPERATOR = ',';
constexpr char KVPAIR_CLAUSE_KV_SEPERATOR = ':';
constexpr char LEVEL_CLAUSE_KV_SEPERATOR[] = ":";
constexpr char LEVEL_CLAUSE_KV_PAIR_SEPERATOR[] = ",";
constexpr char LEVEL_CLAUSE_CLUSTER_LIST_SEPERATOR[] = ";";
constexpr char LEVEL_CLAUSE_CLUSTER_SEPERATOR[] = "&";
constexpr char ANALYZER_CLAUSE_SECTION_KV_PAIR_SEPERATOR[] = ",";
constexpr char ANALYZER_CLAUSE_SECTION_KV_SEPERATOR[] = ":";
constexpr char ANALYZER_CLAUSE_INDEX_KV_PAIR_SEPERATOR[] = "#";
constexpr char ANALYZER_CLAUSE_INDEX_KV_SEPERATOR[] = ";";
constexpr char ANALYZER_CLAUSE_NO_TOKENIZE_SEPERATOR[] = ";";

constexpr char CONFIG_CLAUSE[] = "config";
constexpr char QUERY_CLAUSE[] = "query";
constexpr char FILTER_CLAUSE[] = "filter";
constexpr char PKFILTER_CLAUSE[] = "pkfilter";
constexpr char SORT_CLAUSE[] = "sort";
constexpr char DISTINCT_CLAUSE[] = "distinct";
constexpr char AGGREGATE_CLAUSE[] = "aggregate";
constexpr char RANK_CLAUSE[] = "rank";
constexpr char CLUSTER_CLAUSE[] = "cluster";
constexpr char HEALTHCHECK_CLAUSE[] = "healthcheck";
constexpr char ATTRIBUTE_CLAUSE[] = "attribute";
constexpr char VIRTUALATTRIBUTE_CLAUSE[] = "virtual_attribute";
constexpr char FETCHSUMMARY_CLAUSE[] = "fetch_summary";
constexpr char KVPAIR_CLAUSE[] = "kvpairs";
constexpr char QUERYLAYER_CLAUSE[] = "layer";
constexpr char SEARCHERCACHE_CLAUSE[] = "searcher_cache";
constexpr char OPTIMIZER_CLAUSE[] = "optimizer";
constexpr char LEVEL_CLAUSE[] = "level";
constexpr char ANALYZER_CLAUSE[] = "analyzer";

constexpr char CONFIG_CLAUSE_KEY_CLUSTER[] = "cluster";
constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_CLUSTER[] = "fetch_summary_cluster";
constexpr char CONFIG_CLAUSE_KEY_START[] = "start";
constexpr char CONFIG_CLAUSE_KEY_HIT[] = "hit";
constexpr char CONFIG_CLAUSE_KEY_RANKSIZE[] = "rank_size";
constexpr char CONFIG_CLAUSE_KEY_TOTALRANKSIZE[] = "total_rank_size";
constexpr char CONFIG_CLAUSE_KEY_RERANKSIZE[] = "rerank_size";
constexpr char CONFIG_CLAUSE_KEY_TOTALRERANKSIZE[] = "total_rerank_size";
constexpr char CONFIG_CLAUSE_KEY_FORMAT[] = "format";
constexpr char CONFIG_CLAUSE_KEY_TRACE[] = "trace";
constexpr char CONFIG_CLAUSE_KEY_RANKTRACE[] = "rank_trace";
constexpr char CONFIG_CLAUSE_KEY_USE_CACHE[] = "use_cache";
constexpr char CONFIG_CLAUSE_KEY_ALLOW_LACK_SUMMARY[] = "allow_lack_summary";
constexpr char CONFIG_CLAUSE_KEY_RESULT_COMPRESS[] = "result_compress";
constexpr char CONFIG_CLAUSE_KEY_INNER_RESULT_COMPRESS[] = "inner_result_compress";
constexpr char CONFIG_CLAUSE_KEY_METRICS_SRC[] = "metrics_src";
constexpr char CONFIG_CLAUSE_KEY_KVPAIRS[] = "kvpairs";
constexpr char CONFIG_CLAUSE_KEY_QRS_CHAIN[] = "qrs_chain";
constexpr char CONFIG_CLAUSE_KEY_SUMMARY_PROFILE[] = "summary_profile";
constexpr char CONFIG_CLAUSE_KEY_DEFAULT_INDEX[] = "default_index";
constexpr char CONFIG_CLAUSE_KEY_DEFAULT_OPERATOR[] = "default_operator";
constexpr char CONFIG_CLAUSE_KEY_TIMEOUT[] = "timeout";
constexpr char CONFIG_CLAUSE_KEY_SEEK_TIMEOUT[] = "seek_timeout";
constexpr char CONFIG_CLAUSE_KEY_ANALYZER[] = "analyzer";
constexpr char CONFIG_CLAUSE_KEY_CACHE_INFO[] = "cache_info";
constexpr char CONFIG_CLAUSE_RERANK_HINT[] = "rerank_hint";
constexpr char CONFIG_CLAUSE_KEY_SEARCHER_RETURN_HITS[] = "searcher_return_hits";
constexpr char CONFIG_CLAUSE_KEY_NO_SUMMARY[] = "no_summary";
constexpr char CONFIG_CLAUSE_KEY_DEDUP[] = "dedup";
constexpr char CONFIG_CLAUSE_KEY_SOURCEID[] = "sourceid";
constexpr char CONFIG_CLAUSE_PHASE_ONE_TASK_QUEUE[] = "phase_one_task_queue";
constexpr char CONFIG_CLAUSE_PHASE_TWO_TASK_QUEUE[] = "phase_two_task_queue";
constexpr char CONFIG_CLAUSE_KEY_BATCH_SCORE[] = "batch_score";
constexpr char CONFIG_CLAUSE_KEY_OPTIMIZE_COMPARATOR[] = "optimize_comparator";
constexpr char CONFIG_CLAUSE_KEY_OPTIMIZE_RANK[] = "optimize_rank";
constexpr char CONFIG_CLAUSE_KEY_DEBUG_QUERY_KEY[] = "debug_query_key";


constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_TYPE[] = "fetch_summary_type";
constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_GROUP[] = "fetch_summary_group";
constexpr char CONFIG_CLAUSE_KEY_JOIN_TYPE[] = "join_type";
constexpr char CONFIG_CLAUSE_KEY_JOIN_WEAK_TYPE[] = "weak";
constexpr char CONFIG_CLAUSE_KEY_JOIN_STRONG_TYPE[] = "strong";
constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_DOCID[] = "docid";
constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_PK[] = "pk";
constexpr char CONFIG_CLAUSE_KEY_FETCH_SUMMARY_BY_RAWPK[] = "rawpk";
constexpr char CONFIG_CLAUSE_KEY_PROTO_FORMAT_OPTION[] = "proto_format_option";
constexpr char CONFIG_CLAUSE_KEY_ACTUAL_HITS_LIMIT[] = "actual_hits_limit";
constexpr char CONFIG_CLAUSE_NO_TOKENIZE_INDEXES[] = "no_tokenize_indexes";
constexpr char CONFIG_CLAUSE_RESEARCH_THRESHOLD[] = "research_threshold";
constexpr char CONFIG_CLAUSE_REQUEST_TRACE_TYPE[] = "request_trace_type";
constexpr char CONFIG_CLAUSE_SUB_DOC_DISPLAY_TYPE[] = "sub_doc";
constexpr char CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_NO_TYPE[] = "no";
constexpr char CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_GROUP_TYPE[] = "group";
constexpr char CONFIG_CLAUSE_KEY_SUB_DOC_DISPLAY_FLAT_TYPE[] = "flat";
constexpr char CONFIG_CLAUSE_KEY_IGNORE_DELETE[] = "ignore_delete";
constexpr char CONFIG_CLAUSE_KEY_VERSION[] = "version";
constexpr char CONFIG_CLAUSE_KEY_HIT_SUMMARY_SCHEMA_CACHE_KEY[] = "hit_summary_schema_cache_key";
constexpr char CONFIG_CLAUSE_KEY_GET_ALL_SUB_DOC[] = "get_all_sub_doc";

constexpr char CLUSTER_CLAUSE_KEY_HASH_FIELD[] = "hash_field";
constexpr char CLUSTER_CLAUSE_KEY_PART_IDS[] = "part_ids";

constexpr char SORT_CLAUSE_SEPERATOR[] = ";";
constexpr char SORT_CLAUSE_RANK[] = "RANK";
constexpr char SORT_CLAUSE_DESCEND = '-';
constexpr char SORT_CLAUSE_ASCEND = '+';

constexpr char RANKSORT_CLAUSE[] = "rank_sort";
constexpr char FINALSORT_CLAUSE[] = "final_sort";
constexpr char FINALSORT_SORT_NAME[] = "sort_name";
constexpr char FINALSORT_SORT_PARAM[] = "sort_param";

constexpr char AUX_FILTER_CLAUSE[] = "aux_filter";
constexpr char AUX_QUERY_CLAUSE[] = "aux_query";

constexpr char AGGREGATE_CLAUSE_SEPERATOR[] = ";";
constexpr char AGGREGATE_DESCRIPTION_SEPERATOR[] = ",";
constexpr char AGGREGATE_DESCRIPTION_KV_SEPERATOR[] = ":";
constexpr char AGGREGATE_DESCRIPTION_FUNC_SEPERATOR[] = "#";
constexpr char AGGREGATE_DESCRIPTION_GROUP_KEY[] = "group_key";
constexpr char AGGREGATE_DESCRIPTION_RANGE[] = "range";
constexpr char AGGREGATE_RANGE_SEPERATOR[] = "~";
constexpr char AGGREGATE_DESCRIPTION_AGG_FUN[] = "agg_fun";
constexpr char AGGREGATE_DESCRIPTION_MAX_GROUP[] = "max_group";
constexpr char AGGREGATE_DESCRIPTION_AGG_FILTER[] = "agg_filter";
constexpr char AGGREGATE_DESCRIPTION_AGG_SAMPLER_THRESHOLD[] = "agg_sampler_threshold";
constexpr char AGGREGATE_DESCRIPTION_AGG_SAMPLER_STEP[] = "agg_sampler_step";
constexpr char AGGREGATE_DESCRIPTION_OPEN_PAREN = '(';
constexpr char AGGREGATE_DESCRIPTION_CLOSE_PAREN = ')';
constexpr char AGGREGATE_FUNCTION_COUNT[] = "count";
constexpr char AGGREGATE_FUNCTION_MAX[] = "max";
constexpr char AGGREGATE_FUNCTION_MIN[] = "min";
constexpr char AGGREGATE_FUNCTION_SUM[] = "sum";
constexpr char AGGREGATE_FUNCTION_DISTINCT_COUNT[] = "distinct_count";

constexpr char DISTINCT_CLAUSE_SEPERATOR[] = ";";
constexpr char DISTINCT_DESCRIPTION_SEPERATOR[] = ",";
constexpr char DISTINCT_KV_SEPERATOR[] = ":";
constexpr char DISTINCT_GRADE_SEPERATOR[] = "|";
constexpr char DISTINCT_KEY[] = "dist_key";
constexpr char DISTINCT_COUNT[] = "dist_count";
constexpr char DISTINCT_TIMES[] = "dist_times";
constexpr char DISTINCT_MAX_ITEM_COUNT[] = "max_item_count";
constexpr char DISTINCT_RESERVED_FLAG[] = "reserved";
constexpr char DISTINCT_RESERVED_FLAG_TRUE[] = "true";
constexpr char DISTINCT_RESERVED_FLAG_FALSE[] = "false";
constexpr char DISTINCT_UPDATE_TOTAL_HIT_FLAG[] = "update_total_hit";
constexpr char DISTINCT_UPDATE_TOTAL_HIT_FLAG_TRUE[] = "true";
constexpr char DISTINCT_UPDATE_TOTAL_HIT_FLAG_FALSE[] = "false";
constexpr char DISTINCT_MODULE_NAME[] = "dbmodule";
constexpr char DISTINCT_FILTER[] = "dist_filter";
constexpr char DISTINCT_GRADE[] = "grade";
constexpr char DISTINCT_NONE_DIST[] = "none_dist";

constexpr char RANK_CLAUSE_SEPERATOR[] = ",";
constexpr char RANK_KV_SEPERATOR[] = ":";
constexpr char RANK_RANK_PROFILE[] = "rank_profile";
constexpr char RANK_FIELDBOOST[] = "fieldboost";
constexpr char RANK_HINT[] = "rank_hint";
constexpr char RANK_FIELDBOOST_DESCRIPTION_SEPERATOR[] = ";";
constexpr char RANK_FIELDBOOST_CONFIG_SEPERATOR[] = ".";
constexpr char RANK_FIELDBOOST_CONFIG_OPEN_PAREN[] = "(";
constexpr char RANK_FIELDBOOST_CONFIG_CLOSE_PAREN[] = ")";

constexpr char HEALTHCHECK_CLAUSE_SEPERATOR[] = ",";
constexpr char HEALTHCHECK_KV_SEPERATOR[] = ":";
constexpr char HEALTHCHECK_CHECK_FLAG[] = "check";
constexpr char HEALTHCHECK_CHECK_TIMES[] = "check_times";
constexpr char HEALTHCHECK_CHECK_FLAG_TRUE[] = "true";
constexpr char HEALTHCHECK_CHECK_FLAG_FALSE[] = "false";
constexpr char HEALTHCHECK_RECOVER_TIME[] = "recover_time";

constexpr char FETCHSUMMARY_CLAUSE_SEPERATOR[] = ",";
constexpr char FETCHSUMMARY_CLUSTER_SEPERATOR = ';';
constexpr char FETCHSUMMARY_KV_SEPERATOR = ':';
constexpr char FETCHSUMMARY_RAWPK_SEPERATOR = ',';

constexpr char ATTRIBUTE_CLAUSE_SEPERATOR[] = ",";

constexpr char NO_TOKENIZE_INDEXES_SEPERATOR[] = ";";

constexpr char SEARCHER_CACHE_CLAUSE_USE[] = "use";
constexpr char SEARCHER_CACHE_CLAUSE_KEY[] = "key";
constexpr char SEARCHER_CACHE_CLAUSE_CURTIME[] = "cur_time";
constexpr char SEARCHER_CACHE_CLAUSE_FILTER[] = "cache_filter";
constexpr char SEARCHER_CACHE_CLAUSE_EXPIRE_TIME[] = "expire_time";
constexpr char SEARCHER_CACHE_CLAUSE_CACHE_DOC_NUM[] = "cache_doc_num_limit";

constexpr char SEARCHER_CACHE_USE_YES[] = "yes";
constexpr char SEARCHER_CACHE_USE_NO[] = "no";

constexpr char LEVEL_CLAUSE_KEY_SECOND_LEVEL_CLUSTER[] = "second_level_cluster";
constexpr char LEVEL_CLAUSE_KEY_LEVEL_CLUSTERS[] = "level_clusters";
constexpr char LEVEL_CLAUSE_KEY_USE_LEVEL[] = "uselevel";
constexpr char LEVEL_CLAUSE_KEY_LEVEL_TYPE[] = "level_type";
constexpr char LEVEL_CLAUSE_VALUE_LEVEL_TYPE_BOTH[] = "both";
constexpr char LEVEL_CLAUSE_VALUE_LEVEL_TYPE_GOOD[] = "good";
constexpr char LEVEL_CLAUSE_KEY_MIN_DOCS[] = "min_docs";

constexpr char ANALYZER_CLAUSE_NO_TOKENIZE[] = "no_tokenize_indexes";
constexpr char ANALYZER_CLAUSE_GLOBAL_ANALYZER[] = "global_analyzer";
constexpr char ANALYZER_CLAUSE_SPECIFIC_INDEX_ANALYZER[] = "specific_index_analyzer";

} // namespace common
} // namespace isearch
