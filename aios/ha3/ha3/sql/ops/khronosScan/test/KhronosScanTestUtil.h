#pragma once

#include <unittest/unittest.h>
#include <autil/StringUtil.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>
#include <ha3/common.h>
#include <ha3/sql/ops/khronosScan/KhronosCommon.h>
#include <ha3/sql/data/TableUtil.h>

BEGIN_HA3_NAMESPACE(sql);

class KhronosScanTestUtil {
public:
    typedef std::vector<khronos::DataPoint> DataPointList;
    static void checkDataPointList(const TablePtr &table,
                                   const std::string &colName,
                                   const std::vector<DataPointList> &expected)
    {
        ASSERT_EQ(expected.size(), table->getRowCount());
        ColumnPtr dataColumn = table->getColumn(colName);
        ASSERT_NE(nullptr, dataColumn) << "col [" << colName << "] not exist";
        auto dataColumnTyped = dataColumn->getColumnData<autil::MultiChar>();
        ASSERT_NE(nullptr, dataColumnTyped)  << "col [" << colName << "] not MultiChar";
        for (size_t i = 0; i < expected.size(); ++i) {
            auto &expectedRow = expected[i];
            khronos::DataSeriesReadOnly dataSeries(
                    dataColumnTyped->get(i).data(), dataColumnTyped->get(i).size());
            ASSERT_TRUE(dataSeries.isValid());
            ASSERT_EQ(expectedRow.size(), std::distance(dataSeries.begin(), dataSeries.end()))
                      << "row " << i;

            std::string expectedStr = autil::StringUtil::toString(expectedRow);
            std::string actualStr = autil::StringUtil::toString(
                    dataSeries.begin(), dataSeries.end());
            auto dataSeriesIter = dataSeries.begin();
            for (size_t j = 0; j < expectedRow.size(); ++j) {
                auto dataPoint = *(dataSeriesIter++);
                ASSERT_EQ(expectedRow[j].getTimeStamp(), dataPoint.getTimeStamp())
                    << "i: " << i << std::endl
                    << "j: " << j << std::endl
                    << "expect: " << expectedStr << std::endl
                    << "actual: " << actualStr << std::endl;
                ASSERT_EQ(expectedRow[j].getValue(), dataPoint.getValue())
                    << "i: " << i << std::endl
                    << "j: " << j << std::endl
                    << "expect: " << expectedStr << std::endl
                    << "actual: " << actualStr << std::endl;
            }
        }
    }
};

END_HA3_NAMESPACE();
