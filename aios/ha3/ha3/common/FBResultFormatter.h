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

#include <string>

#include "autil/mem_pool/Pool.h"

#include "ha3/common/Result.h"
#include "ha3/isearch.h"
#include "ha3/proto/SummaryResult_generated.h"
#include "autil/Log.h" // IWYU pragma: keep


namespace isearch {
namespace common {
class SummaryHit;
class Hit;
typedef std::shared_ptr<Hit> HitPtr;
class FlatBufferSimpleAllocator : public flatbuffers::Allocator {
 public:
    explicit FlatBufferSimpleAllocator(autil::mem_pool::Pool *pool) : pool_(pool) {
    }
    uint8_t *allocate(size_t size) override {
        return reinterpret_cast<uint8_t *>(pool_->allocate(size));
    }
    void deallocate(uint8_t *p, size_t size) override{
    }

 private:
    autil::mem_pool::Pool *pool_;
};

class FBResultFormatter
{
public:
    const static uint32_t SUMMARY_IN_BYTES = 1;
    const static uint32_t PB_MATCHDOC_FORMAT = 2;
public:
    FBResultFormatter(autil::mem_pool::Pool *pool);
    ~FBResultFormatter();
public:
    std::string format(const ResultPtr &result);
private:
    SummaryHit *findFirstNonEmptySummaryHit(const std::vector<HitPtr> &hitVec);
private:
    flatbuffers::Offset<isearch::fbs::SummaryResult> CreateSummaryResult(
            const isearch::common::ResultPtr &resultPtr,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<isearch::fbs::FBErrorResult> > >
    CreateErrorResults(
            const isearch::common::MultiErrorResultPtr &multiErrorResult,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<flatbuffers::String> CreateTracer(
            const suez::turing::Tracer *tracer,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::TwoDimTable> CreateSummaryTable(
            const Hits *hits, flatbuffers::FlatBufferBuilder &fbb);

    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByString(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);

    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt8(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt8(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt16(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt16(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt32(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt32(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt64(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt64(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByFloat(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByDouble(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<isearch::common::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
private:
    autil::mem_pool::Pool *_pool;
private:
    friend class FBResultFormatterTest;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace isearch


