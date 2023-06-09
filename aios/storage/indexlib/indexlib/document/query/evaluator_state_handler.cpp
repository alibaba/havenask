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
#include "indexlib/document/query/evaluator_state_handler.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, EvaluatorStateHandler);

EvaluatorStateHandler::EvaluatorStateHandler() {}

EvaluatorStateHandler::~EvaluatorStateHandler() {}

void EvaluatorStateHandler::Init(evaluatorid_t id)
{
    assert(id != INVALID_EVALUATOR_ID);
    if (id >= (evaluatorid_t)mStates.size()) {
        mStates.resize(id + 1, (uint8_t)ES_PENDING);
    }
}

void EvaluatorStateHandler::Init(evaluatorid_t id, const RawDocumentPtr& rawDoc)
{
    autil::StringView stateData = rawDoc->GetEvaluatorState();
    if (stateData.empty()) {
        Init(id);
        return;
    }
    mStates.resize(stateData.size());
    memcpy(mStates.data(), stateData.data(), stateData.size());
    if (id >= (evaluatorid_t)mStates.size()) {
        mStates.resize(id + 1, (uint8_t)ES_PENDING);
    }
}

void EvaluatorStateHandler::UpdateState(evaluatorid_t id, EvaluatorState state)
{
    assert(id != INVALID_EVALUATOR_ID);
    if (id >= (evaluatorid_t)mStates.size()) {
        mStates.resize(id + 1, (uint8_t)ES_PENDING);
    }
    mStates[id] = (uint8_t)state;
}

EvaluatorState EvaluatorStateHandler::GetState(evaluatorid_t id) const
{
    assert(id != INVALID_EVALUATOR_ID);
    if (id >= (evaluatorid_t)mStates.size()) {
        return ES_PENDING;
    }
    return (EvaluatorState)mStates[id];
}

void EvaluatorStateHandler::SyncToDocument(const RawDocumentPtr& rawDoc)
{
    autil::StringView stateData = rawDoc->GetEvaluatorState();
    if (stateData.size() >= mStates.size()) {
        memcpy((void*)stateData.data(), mStates.data(), mStates.size());
        return;
    }

    autil::mem_pool::Pool* pool = rawDoc->getPool();
    char* data = (char*)pool->allocate(mStates.size());
    memcpy(data, mStates.data(), mStates.size());
    autil::StringView newState(data, mStates.size());
    rawDoc->SetEvaluatorState(newState);
}
}} // namespace indexlib::document
