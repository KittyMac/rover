import Foundation
import Hitch

#if canImport(Glibc)
private let posix_lseek = Glibc.lseek
private let posix_write = Glibc.write
private let posix_read = Glibc.read
private let posix_close = Glibc.close
private let posix_pipe = Glibc.pipe
#endif
#if canImport(Darwin)
private let posix_lseek = Darwin.lseek
private let posix_write = Darwin.write
private let posix_read = Darwin.read
private let posix_close = Darwin.close
private let posix_pipe = Darwin.pipe
#endif

fileprivate let headerHitch = Hitch(string: "\u{FFEE}")
fileprivate let headerData = headerHitch.dataCopy()

internal extension Hitchable {
    func writeAll(fd: Int32) -> String? {
        guard let raw = raw() else { return "no raw bytes" }
        var allBytesWritten = 0
        while allBytesWritten < count {
            let bytesWritten = posix_write(fd, raw + allBytesWritten, count - allBytesWritten)
            if bytesWritten < 0 {
                return String(utf8String: strerror(errno))
            }
            allBytesWritten += bytesWritten
        }
        return nil
    }
}

internal extension Data {
    func writeAll(fd: Int32) -> String? {
        return self.withUnsafeBytes { unsafeRawBufferPointer in
            let unsafeBufferPointer = unsafeRawBufferPointer.bindMemory(to: UInt8.self)
            guard let raw = unsafeBufferPointer.baseAddress else { return nil }
            var allBytesWritten = 0
            while allBytesWritten < count {
                let bytesWritten = posix_write(fd, raw + allBytesWritten, count - allBytesWritten)
                if bytesWritten < 0 {
                    return String(utf8String: strerror(errno))
                }
                if bytesWritten == 0 {
                    return nil
                }
                allBytesWritten += bytesWritten
            }
            return nil
        }
    }
}

internal extension Hitch {
    func readAll(fd: Int32, count: Int) -> String? {
        // fputs("PACKET: ---- read bytes start for \(count) bytes ----\n", stderr)
        let buffer = Hitch(capacity: 32768 + 32)
        guard let raw = buffer.mutableRaw() else {
            return "no mutable raw bytes"
        }

        var allBytesRead = 0
        while allBytesRead < count {
            // fputs("PACKET: read bytes: \(buffer.capacity), \(count - allBytesRead)\n", stderr)
            let bytesRead = posix_read(fd, raw, Swift.min(buffer.capacity - 32, count - allBytesRead))
            if bytesRead < 0 {
                return String(utf8String: strerror(errno))
            }
            if bytesRead == 0 {
                return nil
            }
            append(raw, count: bytesRead)
            allBytesRead += bytesRead
        }
        if count != 8 {
            // fputs("PACKET: read: [\(self)]\n", stderr)
        }
        return nil
    }
}

class SafeFileHandle {
    static var standardInput: SafeFileHandle {
        return SafeFileHandle(fd: STDIN_FILENO, closeOnDealloc: false)
    }

    static var standardOutput: SafeFileHandle {
        return SafeFileHandle(fd: STDOUT_FILENO, closeOnDealloc: false)
    }

    static var standardError: SafeFileHandle {
        return SafeFileHandle(fd: STDERR_FILENO, closeOnDealloc: false)
    }
    
    fileprivate var fd: Int32
    fileprivate var closeOnDealloc: Bool = true
    
    var fileDescriptor: Int32 {
        return fd
    }

    init?(forReadingAtPath path: String) {
        fd = open(path, O_RDONLY)
        if fd == -1 {
            return nil
        }
    }

    init?(forWritingAtPath path: String) {
        fd = open(path, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR)
        if fd == -1 {
            return nil
        }
    }

    init?(forUpdatingAtPath path: String) {
        fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR)
        if fd == -1 {
            return nil
        }
    }
    
    init(fd: Int32, closeOnDealloc: Bool) {
        self.fd = fd
        self.closeOnDealloc = closeOnDealloc
    }

    deinit {
        if closeOnDealloc,
           fd != -1 {
            close(fd)
        }
    }

    func readData(ofLength length: Int) -> Data? {
        guard fd != -1 else { return nil }
        var buffer = [UInt8](repeating: 0, count: length)
        let bytesRead = posix_read(fd, &buffer, length)
        if bytesRead <= 0 {
            return nil
        }
        return Data(buffer.prefix(bytesRead))
    }
    
    func read(upToCount count: Int) -> Data? {
        var buffer = [UInt8](repeating: 0, count: count)
        let bytesRead = posix_read(fd, &buffer, count)
        if bytesRead <= 0 {
            return nil
        }
        return Data(buffer.prefix(bytesRead))
    }

    func writeData(_ data: Data) {
        guard fd != -1 else { return }
        data.withUnsafeBytes { (bytes: UnsafeRawBufferPointer) -> Void in
            _ = posix_write(fd, bytes.baseAddress!, data.count)
        }
    }

    func seekToEndOfFile() -> Int64 {
        guard fd != -1 else { return -1 }
        return Int64(posix_lseek(fd, 0, SEEK_END))
    }

    func seek(toFileOffset offset: UInt64) {
        guard fd != -1 else { return }
        _ = posix_lseek(fd, off_t(offset), SEEK_SET)
    }

    func closeFile() {
        guard fd != -1 else { return }
        _ = posix_close(fd)
        fd = -1
    }
    
    func getFD() -> Int32 {
        return fd
    }
}

extension SafeFileHandle {
    
    @discardableResult
    private func fail(_ error: String? = nil) -> (Data?, String?) {
        guard fd >= 0 else { return (nil, error) }
        if let error = error {
            print("SafeFileHandle failed with \(error)")
        }
        _ = posix_close(fd)
        fd = -1
        return (nil, error)
    }
    
    public func writePacket(data: Data) {
        guard fd >= 0 else { fail(); return }
        
        let base64Data = data.base64EncodedData()
        var size: Int = base64Data.count
        let sizeData = Data(bytes: &size,
                            count: MemoryLayout.size(ofValue: size))
        
        // magic header
        if let error = headerData.writeAll(fd: fd) { fail(error); return }
        // packet size
        if let error = sizeData.writeAll(fd: fd) { fail(error); return }
        // payload
        if let error = base64Data.writeAll(fd: fd) { fail(error); return }
    }
    
    public func readPacket() -> (Data?, String?) {
        guard fd >= 0 else { return fail() }

        // magic header
        let headerSize = headerData.count
        let headerBuffer = Hitch(capacity: headerSize)
        if let error = headerBuffer.readAll(fd: fd, count: headerSize) { return fail(error) }
        
        guard headerBuffer == headerHitch else { return fail("header mismatch") }
        
        let size: Int = 0
        let intSize = MemoryLayout.size(ofValue: size)
        let sizeBuffer = Hitch(capacity: intSize)
        guard let sizeRaw = sizeBuffer.mutableRaw() else { return fail() }
        
        // size
        if let error = sizeBuffer.readAll(fd: fd, count: intSize) { return fail(error) }
        let dataSize = sizeRaw.withMemoryRebound(to: Int.self, capacity: 1) { pointer in
            return pointer.pointee
        }
        guard dataSize > 0 else {
            return (nil, "data is empty")
        }
        
        // payload
        let dataBuffer = Hitch(capacity: dataSize)
        if let error = dataBuffer.readAll(fd: fd, count: dataSize) { return fail(error) }
        
        guard dataBuffer.count == dataSize else { return fail("truncated packet \(dataBuffer.count) != \(dataSize)") }
        guard let decodedData = Data(base64Encoded: dataBuffer.dataNoCopy()) else { return fail("failed to base64 decode data") }
        return (decodedData, nil)
    }
    
    public func writePacket(string: String) {
        writePacket(data: string.data(using: .utf8) ?? Data())
    }
    
    public func readPacketAsString() -> (String?, String?) {
        let (data, error) = readPacket()
        if let data = data {
            return (String(data: data, encoding: .utf8), error)
        }
        return (nil, error ?? "unknown error in readPacketAsString")
    }
    
    public func writePacket(hitch: Hitch) {
        writePacket(data: hitch.dataNoCopy())
    }
    
    public func writePacket(halfhitch: HalfHitch) {
        writePacket(data: halfhitch.dataNoCopy())
    }
    
    public func write(hitch: Hitch) {
        _ = hitch.writeAll(fd: fd)
    }
    
    public func readPacketAsHitch() -> (Hitch?, String?) {
        let (data, error) = readPacket()
        if let data = data {
            return (Hitch(data: data), error)
        }
        return (nil, error ?? "unknown error in readPacketAsHitch")
    }
}
