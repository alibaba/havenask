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
#include "indexlib/index/data_structure/var_len_data_dumper.h"

#include <unordered_map>

#include "autil/ConstString.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, VarLenDataDumper);

VarLenDataDumper::VarLenDataDumper() : mAccessor(nullptr), mDataItemCount(0), mMaxItemLen(0) {}

VarLenDataDumper::~VarLenDataDumper() {}

bool VarLenDataDumper::Init(VarLenDataAccessor* accessor, const VarLenDataParam& dumpParam)
{
    mAccessor = accessor;
    mOutputParam = dumpParam;
    if (mAccessor == nullptr) {
        IE_LOG(ERROR, "accessor should not be null.");
        return false;
    }
    if (mAccessor->IsUniqEncode() != dumpParam.dataItemUniqEncode) {
        IE_LOG(ERROR, "accessor & dumpParam shoud has same uniq encode param.");
        return false;
    }
    return true;
}

void VarLenDataDumper::Dump(const DirectoryPtr& directory, const string& offsetFileName, const string& dataFileName,
                            const FSWriterParamDeciderPtr& fsWriterParamDecider, std::vector<docid_t>* newOrder,
                            PoolBase* dumpPool)
{
    VarLenDataWriter dataWriter(dumpPool);
    dataWriter.Init(directory, offsetFileName, dataFileName, fsWriterParamDecider, mOutputParam);
    dataWriter.Reserve(mAccessor->GetDocCount());

    uint32_t appendSize = mAccessor->GetAppendDataSize();
    if (mOutputParam.appendDataItemLength) {
        appendSize += sizeof(uint32_t) * mAccessor->GetDocCount();
    }
    dataWriter.GetDataFileWriter()->ReserveFile(appendSize).GetOrThrow();
    DumpToWriter(dataWriter, newOrder);
    dataWriter.Close();
}

void VarLenDataDumper::DumpToWriter(VarLenDataWriter& dataWriter, std::vector<docid_t>* newOrder)
{
    assert(mAccessor);
    if (newOrder) {
        assert(newOrder->size() == mAccessor->GetDocCount());
        for (docid_t newDocId = 0; newDocId < mAccessor->GetDocCount(); ++newDocId) {
            docid_t oldDocId = newOrder->at(newDocId);
            uint64_t offset = mAccessor->GetOffset(oldDocId);
            uint8_t* data = NULL;
            uint32_t dataLength = 0;
            [[maybe_unused]] bool ret = mAccessor->ReadData(oldDocId, data, dataLength);
            assert(ret);
            dataWriter.AppendValue(StringView((const char*)data, dataLength), offset);
        }
    } else {
        VarLenDataIteratorPtr iter = mAccessor->CreateDataIterator();
        while (iter->HasNext()) {
            uint64_t offset = 0;
            uint8_t* data = NULL;
            uint64_t dataLength = 0;
            iter->Next();
            iter->GetCurrentOffset(offset);
            iter->GetCurrentData(dataLength, data);
            dataWriter.AppendValue(StringView((const char*)data, dataLength), offset);
        }
    }
    size_t appendSize = mAccessor->GetAppendDataSize();
    if (mOutputParam.appendDataItemLength) {
        appendSize += sizeof(uint32_t) * mAccessor->GetDocCount();
    }
    assert(dataWriter.GetDataFileWriter()->GetLogicLength() <= appendSize);
    mDataItemCount = dataWriter.GetDataItemCount();
    mMaxItemLen = dataWriter.GetMaxItemLength();
}
}} // namespace indexlib::index
