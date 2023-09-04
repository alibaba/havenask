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
#include <aitheta2/index_error.h>
#include <aitheta2/index_factory.h>
#include <aitheta2/index_format.h>
#include <aitheta2/index_packer.h>
#include <cerrno>

#include "indexlib/index/ann/aitheta2/CommonDefine.h"
#include "indexlib/index/ann/aitheta2/util/segment_data/SegmentDataWriter.h"

namespace aitheta2 {
/*! Index Segment Dumper
 */
class CustomizedAiThetaDumper : public IndexDumper
{
public:
    //! Index Segment Dumper Pointer
    typedef std::shared_ptr<CustomizedAiThetaDumper> Pointer;

    CustomizedAiThetaDumper(const indexlibv2::index::ann::IndexDataWriterPtr& writer) : writer_(writer) {}

    //! Destructor
    ~CustomizedAiThetaDumper(void) { this->close(); }

    //! Initialize dumper
    int init(const IndexParams&) override { return 0; }

    //! Cleanup dumper
    int cleanup(void) override { return 0; }

    //! Create a file for dumping
    int create(const std::string&) override
    {
        if (dumped_size_ != 0) {
            return IndexError_NoReady;
        }

        auto write_data = [&](const void* buf, size_t size) { return this->write_to_dumper(buf, size); };
        if (!packer_.setup(write_data)) {
            return IndexError_WriteData;
        }
        return 0;
    }

    //! Close file
    int close(void) override { return this->close_index(); }

    //! Append a segment meta into table
    int append(const std::string& id, size_t data_size, size_t padding_size, uint32_t crc) override
    {
        stab_.emplace_back(id, data_size, padding_size, crc);
        return 0;
    }

    //! Write data to the storage
    size_t write(const void* data, size_t len) override
    {
        auto write_data = [&](const void* buf, size_t size) { return this->write_to_dumper(buf, size); };

        if (dumped_size_ == 0 && !packer_.setup(write_data)) {
            return 0;
        }
        return packer_.pack(write_data, data, len);
    }

    //! Retrieve magic number of index
    uint32_t magic(void) const override { return packer_.magic(); }

public:
    static int dump(const indexlibv2::index::ann::AiThetaBuilderPtr& builder,
                    indexlibv2::index::ann::IndexDataWriterPtr& writer)
    {
        auto dumper = create(writer);
        if (!dumper) {
            return aitheta2::IndexError_InvalidArgument;
        }
        int ret = builder->dump(dumper);
        if (ret != 0) {
            return ret;
        }
        return dumper->close();
    }

    static int dump(const indexlibv2::index::ann::AiThetaStreamerPtr& streamer,
                    indexlibv2::index::ann::IndexDataWriterPtr& writer)
    {
        auto dumper = create(writer);
        if (!dumper) {
            return aitheta2::IndexError_InvalidArgument;
        }
        int ret = streamer->dump(dumper);
        if (ret != 0) {
            return ret;
        }
        return dumper->close();
    }

protected:
    static IndexDumper::Pointer create(const indexlibv2::index::ann::IndexDataWriterPtr& writer)
    {
        auto dumper = IndexDumper::Pointer(new CustomizedAiThetaDumper(writer));
        IndexParams params;
        int error = dumper->init(params);
        if (error < 0) {
            LOG_ERROR("dumper init failed, error[%s]", IndexError::What(error));
            return Pointer();
        }
        std::string segid;
        error = dumper->create(segid);
        if (error < 0) {
            LOG_ERROR("dumper create failed, error[%s]", IndexError::What(error));
            return Pointer();
        }
        return dumper;
    }

protected:
    //! Write data to dumper
    size_t write_to_dumper(const void* data, size_t len)
    {
        size_t wrlen = writer_->Write(data, len);
        dumped_size_ += wrlen;
        return wrlen;
    }

    //! Close index file
    int close_index(void)
    {
        if (dumped_size_ == 0) {
            writer_->Close();
            return 0;
        }

        auto write_data = [&](const void* buf, size_t size) { return this->write_to_dumper(buf, size); };
        if (!packer_.finish(write_data, stab_)) {
            return IndexError_WriteData;
        }
        writer_->Close();
        stab_.clear();
        dumped_size_ = 0u;
        return 0;
    }

private:
    std::string segid_;
    size_t dumped_size_ {0};
    indexlibv2::index::ann::IndexDataWriterPtr writer_ {};
    IndexPacker packer_ {};
    std::vector<IndexPacker::SegmentMeta> stab_ {};
};

} // namespace aitheta2
