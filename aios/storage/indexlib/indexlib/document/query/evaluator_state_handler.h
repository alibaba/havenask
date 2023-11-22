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
#include "indexlib/document/raw_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

typedef int32_t evaluatorid_t;
const evaluatorid_t INVALID_EVALUATOR_ID = (evaluatorid_t)-1;

enum EvaluatorState {
    ES_PENDING = 0,
    ES_FALSE = 1,
    ES_TRUE = 2,
};

class EvaluatorStateHandler
{
public:
    EvaluatorStateHandler();
    ~EvaluatorStateHandler();

public:
    void Init(evaluatorid_t id);
    void Init(evaluatorid_t id, const RawDocumentPtr& rawDoc);

    void UpdateState(evaluatorid_t id, EvaluatorState state);
    EvaluatorState GetState(evaluatorid_t id) const;

    void SyncToDocument(const RawDocumentPtr& rawDoc);

private:
    std::vector<uint8_t> mStates;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(EvaluatorStateHandler);
}} // namespace indexlib::document
