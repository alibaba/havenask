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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_init_param.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"

DECLARE_REFERENCE_CLASS(document, DocumentRewriter);
DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace indexlib { namespace document {

struct BuiltInParserInitResource {
    util::CounterMapPtr counterMap;
    util::MetricProviderPtr metricProvider = nullptr;
    std::vector<DocumentRewriterPtr> docRewriters;
};

typedef DocumentInitParamTyped<BuiltInParserInitResource> BuiltInParserInitParam;
DEFINE_SHARED_PTR(BuiltInParserInitParam);
}} // namespace indexlib::document
