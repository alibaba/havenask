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
#include "indexlib/index/kkv/codegen_kkv_reader.h"

#include "indexlib/index/kkv/kkv_reader_impl_factory.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, CodegenKKVReader);

CodegenKKVReader::CodegenKKVReader() : KKVReader() {}

CodegenKKVReader::CodegenKKVReader(const CodegenKKVReader& rhs) : KKVReader(rhs)
{
    mReader = rhs.mReader;
    mReaderType = rhs.mReaderType;
}

CodegenKKVReader::~CodegenKKVReader() {}

void CodegenKKVReader::Open(const KKVIndexConfigPtr& kkvConfig, const PartitionDataPtr& partitionData,
                            const SearchCachePartitionWrapperPtr& searchCache, int64_t latestNeedSkipIncTs,
                            const string& schemaName)
{
    mPrefixKeyHasherType = kkvConfig->GetPrefixHashFunctionType();
    mSuffixKeyHasherType = kkvConfig->GetSuffixHashFunctionType();
    mReader.reset((KKVReaderImplType*)KKVReaderImplFactory::Create(kkvConfig, schemaName, mReaderType));
    mReader->Open(kkvConfig, partitionData, searchCache);

    mKKVIndexOptions.Init(kkvConfig, partitionData, latestNeedSkipIncTs);
}

void CodegenKKVReader::ReInit(const KKVIndexConfigPtr& kkvConfig, const PartitionDataPtr& partitionData,
                              int64_t latestNeedSkipIncTs)
{
    mKKVIndexOptions = KKVIndexOptions();
    mPrefixKeyHasherType = kkvConfig->GetPrefixHashFunctionType();
    mSuffixKeyHasherType = kkvConfig->GetSuffixHashFunctionType();

    mKKVIndexOptions.Init(kkvConfig, partitionData, latestNeedSkipIncTs);
}

KKVReaderPtr CodegenKKVReader::CreateCodegenKKVReader()
{
    KKVReaderPtr codegenReader;
    CodegenObject* codegenObject = nullptr;
    using createFunc = CodegenObject*();
    CREATE_CODEGEN_OBJECT(codegenObject, createFunc, KKVReader);
    if (codegenObject) {
        codegenReader.reset((KKVReader*)codegenObject);
    }
    return codegenReader;
}

bool CodegenKKVReader::doCollectAllCode()
{
    INIT_CODEGEN_INFO(CodegenKKVReader, true);
    mReader->collectAllCode();
    codegen::CodegenInfo info;
    KKVReaderImplFactory::CollectAllCode(info);
    mSubCodegenInfos.push_back(info);
    combineCodegenInfos(mReader.get());
    COLLECT_TYPE_REDEFINE(KKVReaderImplType, mReaderType);
    return true;
}

void CodegenKKVReader::TEST_collectCodegenResult(CodegenCheckers& checkers, std::string id)
{
    codegen::CodegenCheckerPtr checker(new codegen::CodegenChecker);
    COLLECT_TYPE_DEFINE(checker, KKVReaderImplType);
    checkers["CodegenKKVReader"] = checker;
    mReader->TEST_collectCodegenResult(checkers, id);
}
}} // namespace indexlib::index
