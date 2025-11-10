"""Demonstrates the POSIX-like API for AFS client."""
import asyncio
from DS_DFS_AFSlike.rpc.client import RPCClient
from DS_DFS_AFSlike.afs.client import AFSClient

async def main():
    print("AFS POSIX-like API Demo\n")
    
    rpc = RPCClient(["127.0.0.1:8888"])
    afs = AFSClient(rpc)
    
    #Example: Create and write a new file
    print("Creating new file /test_d.txt")
    fd1 = await afs.create("/test_d.txt")
    print(f"File descriptor: {fd1}")
    
    print("Writing data to file")
    data = b"This is a test file.\n"
    bytes_written = afs.write(fd1, data)
    print(f"Wrote {bytes_written} bytes")
    
    print("Closing file")
    await afs.close(fd1)
    print("File closed and flushed\n")
    
    #Example: Open file and read
    print("Opening /test_d.txt for reading")
    fd2 = await afs.open("/test_d.txt", mode='r')
    print(f"File descriptor: {fd2}")
    
    print("Reading file content")
    content = afs.read(fd2)
    print(f"Read: {content.decode('utf-8')}")
    
    print("Closing file")
    await afs.close(fd2)
    print("File closed\n")
    
    #Append to existing file
    print("Opening /test.txt for read/write")
    fd3 = await afs.open("/test.txt", mode='r+')
    print(f"File descriptor: {fd3}")
    
    print("Find end")
    afs.seek(fd3, 0, 2)
    
    print("Appending more data")
    more_data = b"This is a new line.\n"
    afs.write(fd3, more_data)
    print(f"Appended {len(more_data)} bytes")
    
    print("Closing file")
    await afs.close(fd3)
    print("File closed and flushed\n")
    print("\nDemo Complete")

if __name__ == "__main__":
    asyncio.run(main())