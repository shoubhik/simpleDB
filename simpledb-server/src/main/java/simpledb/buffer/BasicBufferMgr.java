package simpledb.buffer;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;

/**
 * User: shoubhik Date: 29/3/13 Time: 1:00 AM
 */
final class BasicBufferMgr implements IBufferMgr {


    private GClockBufferPool gClockBufferPool;
    private static int MAX_ROTATIONS_ALLOWED = 10;
    private static int MAX_TICKS = 5;
    private Map<Block, Buffer> bufferPoolMap;

    BasicBufferMgr(int numbuffs){
        gClockBufferPool = new GClockBufferPool(numbuffs, MAX_ROTATIONS_ALLOWED,
                                                MAX_TICKS);
        bufferPoolMap = new HashMap<Block, Buffer>();
    }
    @Override
    public synchronized void flushAll(int txnum) {
        this.gClockBufferPool.flushAll(txnum);
    }

    @Override
    public synchronized Buffer pin(Block blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = this.gClockBufferPool.chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            if(!blk.equals(buff.block()))
                bufferPoolMap.remove(buff.block());
            buff.assignToBlock(blk);
            bufferPoolMap.put(blk, buff);
        }
        if (!buff.isPinned())
            this.gClockBufferPool.decrementAvailable();
        buff.pin();
        return buff;
    }

    @Override
    public synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
        Buffer buff = this.gClockBufferPool.chooseUnpinnedBuffer();
        if (buff == null)
            return null;
        buff.assignToNew(filename, fmtr);
        this.gClockBufferPool.decrementAvailable();
        buff.pin();
        bufferPoolMap.put(buff.block(), buff);
        return buff;
    }

    @Override
    public synchronized void unpin(Buffer buff) {
        buff.unpin();
        if (!buff.isPinned())
            this.gClockBufferPool.incrementAvailable();
    }

    @Override
    public int available() {
        return this.gClockBufferPool.getNumAvailable();
    }
    private synchronized Buffer findExistingBuffer(Block blk) {
        return bufferPoolMap.get(blk);
    }

    /**
     * Determines whether the map has a mapping from
     * the block to some buffer.
     * @param blk the block to use as a key
     * @return true if there is a mapping; false otherwise
     */
    public boolean containsMapping(Block blk) {
        return bufferPoolMap.containsKey(blk);
    }
    /**
     * Returns the buffer that the map maps the specified block to.
     * @param blk the block to use as a key
     * @return the buffer mapped to if there is a mapping; null otherwise
     */
    public Buffer getMapping(Block blk) {
        return bufferPoolMap.get(blk);
    }

    // for testing purpose only
    public void setBufferPool(GClockBufferPool gClockBufferPool){
        this.gClockBufferPool = gClockBufferPool;
    }

    public GClockBufferPool getBufferPool(){
        return this.gClockBufferPool;
    }
}
