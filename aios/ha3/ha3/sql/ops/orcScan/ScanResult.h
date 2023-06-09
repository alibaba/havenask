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

#include "navi/engine/Data.h"
#include "table/Table.h"

namespace indexlib2::index {
class RowGroup;
}

namespace isearch {
namespace sql {


static const std::string SCAN_RESULT_DATA_TYPE = "ha3.sql.scan_result_data_type";

enum class ScanState {
    SS_INIT,
    SS_SCAN ,
    SS_FINISH
};

class ScanResult : public navi::Data {
public:
    ScanResult(ScanState scanState, indexlibv2::Status dataStatus, std::unique_ptr<indexlibv2::index::RowGroup> rowGroup)
        : navi::Data(SCAN_RESULT_DATA_TYPE, nullptr)
        , _scanState(scanState)
        , _dataStatus(std::move(dataStatus))
        , _rowGroup(std::move(rowGroup))
    {
    }
    ScanResult(ScanState scanState, indexlibv2::Status dataStatus)
        : navi::Data(SCAN_RESULT_DATA_TYPE, nullptr)
        , _scanState(scanState)
        , _dataStatus(std::move(dataStatus))
    {
    }
    ScanResult(ScanState scanState)
        : navi::Data(SCAN_RESULT_DATA_TYPE, nullptr)
        , _scanState(scanState)
    {
    }

    ~ScanResult() {}

private:
    ScanResult(const ScanResult&);
    ScanResult &operator=(const ScanResult&);

public:
    ScanState getScanState() const {
        return _scanState;
    }
    indexlibv2::Status getDataStatus() const {
        return _dataStatus;
    }

    indexlibv2::index::RowGroup *getRowGroup() const {
        return _rowGroup.get();
    }

private:
    ScanState _scanState;
    indexlibv2::Status _dataStatus;
    std::unique_ptr<indexlibv2::index::RowGroup> _rowGroup;
};

}
}
