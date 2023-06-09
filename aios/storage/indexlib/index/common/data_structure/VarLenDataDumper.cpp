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
#include "indexlib/index/common/data_structure/VarLenDataDumper.h"

#include <unordered_map>

#include "autil/ConstString.h"

using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::file_system;

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, VarLenDataDumper);

VarLenDataDumper::VarLenDataDumper() : _accessor(nullptr), _dataItemCount(0), _maxItemLen(0) {}

VarLenDataDumper::~VarLenDataDumper() {}

bool VarLenDataDumper::Init(VarLenDataAccessor* accessor, const VarLenDataParam& dumpParam)
{
    _accessor = accessor;
    _outputParam = dumpParam;
    if (_accessor == nullptr) {
        AUTIL_LOG(ERROR, "accessor should not be null.");
        return false;
    }
    if (_accessor->IsUniqEncode() != dumpParam.dataItemUniqEncode) {
        AUTIL_LOG(ERROR, "accessor & dumpParam shoud has same uniq encode param.");
        return false;
    }
    return true;
}

Status VarLenDataDumper::Dump(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                              const std::string& offsetFileName, const std::string& dataFileName,
                              const std::shared_ptr<indexlib::index::FSWriterParamDecider>& fsWriterParamDecider,
                              std::vector<docid_t>* newOrder, PoolBase* dumpPool)
{
    VarLenDataWriter dataWriter(dumpPool);
    auto st = dataWriter.Init(directory, offsetFileName, dataFileName, fsWriterParamDecider, _outputParam);
    RETURN_IF_STATUS_ERROR(st, "init var len data writer failed.");
    dataWriter.Reserve(_accessor->GetDocCount());

    uint32_t appendSize = _accessor->GetAppendDataSize();
    if (_outputParam.appendDataItemLength) {
        appendSize += sizeof(uint32_t) * _accessor->GetDocCount();
    }
    st = dataWriter.GetDataFileWriter()->ReserveFile(appendSize).Status();
    RETURN_IF_STATUS_ERROR(st, "reserve file failed, size[%u]", appendSize);
    st = DumpToWriter(dataWriter, newOrder);
    RETURN_IF_STATUS_ERROR(st, "dump to writer failed.");
    st = dataWriter.Close();
    RETURN_IF_STATUS_ERROR(st, "close data writer failed.");
    return Status::OK();
}

Status VarLenDataDumper::DumpToWriter(VarLenDataWriter& dataWriter, std::vector<docid_t>* newOrder)
{
    assert(_accessor);
    if (newOrder) {
        assert(newOrder->size() == _accessor->GetDocCount());
        for (docid_t newDocId = 0; newDocId < _accessor->GetDocCount(); ++newDocId) {
            docid_t oldDocId = newOrder->at(newDocId);
            uint64_t offset = _accessor->GetOffset(oldDocId);
            uint8_t* data = NULL;
            uint32_t dataLength = 0;
            [[maybe_unused]] bool ret = _accessor->ReadData(oldDocId, data, dataLength);
            assert(ret);
            auto st = dataWriter.AppendValue(StringView((const char*)data, dataLength), offset);
            RETURN_IF_STATUS_ERROR(st, "append value failed.");
        }
    } else {
        std::shared_ptr<VarLenDataIterator> iter = _accessor->CreateDataIterator();
        while (iter->HasNext()) {
            uint64_t offset = 0;
            uint8_t* data = NULL;
            uint64_t dataLength = 0;
            iter->Next();
            iter->GetCurrentOffset(offset);
            iter->GetCurrentData(dataLength, data);
            auto st = dataWriter.AppendValue(StringView((const char*)data, dataLength), offset);
            RETURN_IF_STATUS_ERROR(st, "append value failed.");
        }
    }
    size_t appendSize = _accessor->GetAppendDataSize();
    if (_outputParam.appendDataItemLength) {
        appendSize += sizeof(uint32_t) * _accessor->GetDocCount();
    }
    assert(dataWriter.GetDataFileWriter()->GetLogicLength() <= appendSize);
    _dataItemCount = dataWriter.GetDataItemCount();
    _maxItemLen = dataWriter.GetMaxItemLength();
    return Status::OK();
}
} // namespace indexlibv2::index
