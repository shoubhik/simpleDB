package simpledb.buffer;

import simpledb.file.Block;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * User: shoubhik Date: 29/3/13 Time: 1:00 AM
 */
final class GClockBufferMgr implements IBufferMgr {


    private GClockBufferPool gClockBufferPool;
    private static int MAX_ROTATIONS_ALLOWED = 5;
    private static int MAX_TICKS = 5;
    private Map<Block, Buffer> bufferMap;

    GClockBufferMgr(int numbuffs){
        gClockBufferPool = new GClockBufferPool(numbuffs, MAX_ROTATIONS_ALLOWED,
                                                MAX_TICKS);
        bufferMap = new HashMap<Block, Buffer>();
    }
    @Override
    public synchronized void flushAll(int txnum) {
        this.gClockBufferPool.flushAll(txnum);
    }

    @Override
    public synchronized Buffer pin(Block blk) {
        Buffer buff = findExistingBuffer(blk);
        if(buff != null)
            System.out.printf("buffer existing for block %d\n", blk.number());
        if (buff == null) {
            buff = this.gClockBufferPool.chooseUnpinnedBuffer();
            if (buff == null)
                return null;
//            buff.flush();
            if(!blk.equals(buff.block()))
                bufferMap.remove(buff.block());
            buff.assignToBlock(blk);
            bufferMap.put(blk, buff);
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
        bufferMap.put(buff.block(), buff);
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
        return bufferMap.get(blk);
    }

}
