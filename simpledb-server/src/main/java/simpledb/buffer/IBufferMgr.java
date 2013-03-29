package simpledb.buffer;

import simpledb.file.Block;

/**
 * User: shoubhik Date: 29/3/13 Time: 12:17 AM
 */
interface IBufferMgr {
    void flushAll(int txnum);
    Buffer pin(Block blk) ;
    Buffer pinNew(String filename, PageFormatter fmtr);
    void unpin(Buffer buff);
    int available();

}
