// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "io/fs/local_file_writer.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <fcntl.h>
#include <glog/logging.h>
#include <limits.h>
#include <stdint.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "cached_local_file_writer3.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "gutil/macros.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "util/doris_metrics.h"

namespace doris {
namespace detail {

Status sync_dir(const io::Path& dirname) {
    int fd;
    RETRY_ON_EINTR(fd, ::open(dirname.c_str(), O_DIRECTORY | O_RDONLY));
    if (-1 == fd) {
        return Status::IOError("cannot open {}: {}", dirname.native(), std::strerror(errno));
    }
#ifdef __APPLE__
    if (fcntl(fd, F_FULLFSYNC) < 0) {
        return Status::IOError("cannot sync {}: {}", dirname.native(), std::strerror(errno));
    }
#else
    if (0 != ::fdatasync(fd)) {
        return Status::IOError("cannot fdatasync {}: {}", dirname.native(), std::strerror(errno));
    }
#endif
    ::close(fd);
    return Status::OK();
}

} // namespace detail

namespace io {

CachedLocalFileWriter3::CachedLocalFileWriter3(Path path, int fd, FileSystemSPtr fs)
        : FileWriter(std::move(path), fs), _fd(fd) {
    _opened = true;
    DorisMetrics::instance()->local_file_open_writing->increment(1);
    DorisMetrics::instance()->local_file_writer_total->increment(1);
}

CachedLocalFileWriter3::CachedLocalFileWriter3(Path path, int fd)
        : CachedLocalFileWriter3(path, fd, global_local_filesystem()) {}

CachedLocalFileWriter3::~CachedLocalFileWriter3() {
    if (_opened) {
        close();
    }
    CHECK(!_opened || _closed) << "open: " << _opened << ", closed: " << _closed;
}

Status CachedLocalFileWriter3::close() {
    return _close(true);
}

Status CachedLocalFileWriter3::abort() {
    RETURN_IF_ERROR(_close(false));
    return io::global_local_filesystem()->delete_file(_path);
}

Status CachedLocalFileWriter3::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(!_closed);
    _dirty = true;

    // Convert the results into the iovec vector to request
    // and calculate the total bytes requested.
    size_t bytes_req = 0;

    // 合并调用字节
    std::vector<char> merged_data;
    for (size_t i = 0; i < data_cnt; i++) {
        const Slice& result = data[i];
        bytes_req += result.size;
        merged_data.insert(merged_data.end(), result.data, result.data + result.size);
    }

    size_t offset = 0;
    std::vector<iovec> iov;
    while (offset < merged_data.size()) {
        const size_t size_to_write = std::min(static_cast<size_t>(IOV_MAX), merged_data.size() - offset);
        iov.push_back({&merged_data[offset], size_to_write});
        offset += size_to_write;
    }

    ssize_t res;
    RETRY_ON_EINTR(res, ::writev(_fd, iov.data(), iov.size()));
    if (UNLIKELY(res < 0)) {
        LOG(ERROR) << "can not write to " << _path.native() << ", error no: " << errno
                  << ", error msg: " << strerror(errno);
        return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
    }

    DCHECK_EQ(static_cast<size_t>(res), bytes_req);

    _bytes_appended += bytes_req;
    return Status::OK();
}

Status CachedLocalFileWriter3::write_at(size_t offset, const Slice& data) {
    DCHECK(!_closed);
    _dirty = true;

    size_t bytes_req = data.size;
    char* from = data.data;

    while (bytes_req != 0) {
        auto res = ::pwrite(_fd, from, bytes_req, offset);
        if (-1 == res && errno != EINTR) {
            return Status::IOError("cannot write to {}: {}", _path.native(), std::strerror(errno));
        }
        if (res > 0) {
            from += res;
            bytes_req -= res;
        }
    }
    return Status::OK();
}

Status CachedLocalFileWriter3::finalize() {
    DCHECK(!_closed);
    if (_dirty) {
#if defined(__linux__)
        int flags = SYNC_FILE_RANGE_WRITE;
        if (sync_file_range(_fd, 0, 0, flags) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
    }
    return Status::OK();
}

Status CachedLocalFileWriter3::_close(bool sync) {
    if (_closed) {
        return Status::OK();
    }
    _closed = true;
    if (sync && _dirty) {
#ifdef __APPLE__
        if (fcntl(_fd, F_FULLFSYNC) < 0) {
            return Status::IOError("cannot sync {}: {}", _path.native(), std::strerror(errno));
        }
#else
        if (0 != ::fdatasync(_fd)) {
            return Status::IOError("cannot fdatasync {}: {}", _path.native(), std::strerror(errno));
        }
#endif
        RETURN_IF_ERROR(detail::sync_dir(_path.parent_path()));
        _dirty = false;
    }

    DorisMetrics::instance()->local_file_open_writing->increment(-1);
    DorisMetrics::instance()->file_created_total->increment(1);
    DorisMetrics::instance()->local_bytes_written_total->increment(_bytes_appended);

    if (0 != ::close(_fd)) {
        return Status::IOError("cannot close {}: {}", _path.native(), std::strerror(errno));
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
