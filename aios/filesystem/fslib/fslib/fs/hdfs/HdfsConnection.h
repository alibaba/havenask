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
#ifndef FSLIB_PLUGIN_HDFSCONNECTION_H
#define FSLIB_PLUGIN_HDFSCONNECTION_H

#include <hdfs/hdfs.h>
#include <memory>

#include "autil/Log.h"
#include "fslib/common.h"

FSLIB_PLUGIN_BEGIN_NAMESPACE(hdfs);

class HdfsConnection {
public:
    HdfsConnection(hdfsFS fs);
    ~HdfsConnection();

    hdfsFS getHdfsFs() { return _fs; }
    void setError(bool hasError) { _hasError = hasError; }
    bool hasError() { return _hasError; }

private:
    hdfsFS _fs;
    bool _hasError;
};

typedef std::shared_ptr<HdfsConnection> HdfsConnectionPtr;

FSLIB_PLUGIN_END_NAMESPACE(fs);

#endif // FSLIB_PLUGIN_HDFSCONNECTION_H
