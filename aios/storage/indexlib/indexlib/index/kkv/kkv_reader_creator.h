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
#ifndef __INDEXLIB_KKVREADERCREATOR_H
#define __INDEXLIB_KKVREADERCREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/kkv/codegen_kkv_reader.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KKVReaderCreator
{
public:
    static KKVReaderPtr CreateAdaptiveKKVReader(config::KKVIndexConfigPtr kkvConfig,
                                                const index_base::PartitionDataPtr& partitionData,
                                                const util::SearchCachePartitionWrapperPtr& searchCache,
                                                int64_t latestNeedSkipIncTs, const std::string& schemaName)
    {
        KKVReaderPtr reader(new CodegenKKVReader);
        reader->Open(kkvConfig, partitionData, searchCache, latestNeedSkipIncTs, schemaName);
        auto codegenKKVReader = reader->CreateCodegenKKVReader();
        if (codegenKKVReader) {
            codegenKKVReader->Open(kkvConfig, partitionData, searchCache, latestNeedSkipIncTs, schemaName);
            reader = codegenKKVReader;
        } else {
            IE_LOG(WARN, "create codegen kkv reader failed, schema name [%s]", schemaName.c_str());
        }
        return reader;
    }

private:
    IE_LOG_DECLARE();
};
IE_LOG_SETUP(index, KKVReaderCreator);
}} // namespace indexlib::index

#endif //__INDEXLIB_KKVREADERCREATOR_H
