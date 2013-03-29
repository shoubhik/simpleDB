package simpledb.buffer;

import simpledb.file.Block;

/**
 * User: shoubhik Date: 29/3/13 Time: 1:01 AM
 */
public class GClockBufferPool {
    private Buffer[] bufferpool;
    private int numAvailable;
    private int maxRotationAllowed;
    private int currBuffIdx;
    private int buffSize;
    private int maxTicks;

    public GClockBufferPool(int numbuffs, int maxRotationAllowed, int maxTicks){
        bufferpool = new Buffer[numbuffs];
        numAvailable = numbuffs;
        buffSize = numbuffs;
        for (int i=0; i<numbuffs; i++)
            bufferpool[i] = new Buffer(i);
        this.maxRotationAllowed = maxRotationAllowed;
        this.currBuffIdx = 0;
        this.maxTicks = maxTicks;
    }

    public synchronized Buffer chooseUnpinnedBuffer(){
        //doing this to stop overflow
        currBuffIdx %= buffSize;
        int numRotations = 0;
        while(numRotations <= maxRotationAllowed){
            for(int i = currBuffIdx; i < numAvailable + currBuffIdx ; i++){
                Buffer buffer = bufferpool[i%buffSize];
                if(!buffer.isPinned() &&
                        buffer.getReferenceCount() == 0){
                    buffer.setReferenceCount(maxTicks);
                    currBuffIdx = i+1;
                    return buffer;
                }
                else if(!buffer.isPinned() && buffer.getReferenceCount() > 0){
                    buffer.decrementReferenceCount();
                }
            }
            numRotations++;
        }
        System.out.printf("no buffers found after %d rotations ",
                          maxRotationAllowed);
        return null;
    }

    public synchronized void flushAll(int txnum){
        for (Buffer buff : bufferpool)
            if (buff.isModifiedBy(txnum))
                buff.flush();
    }

    public void decrementAvailable(){
        --numAvailable;
    }

    public void incrementAvailable(){
        ++numAvailable;
    }

    public int getNumAvailable() {
        return numAvailable;
    }

    public Buffer exiting(Block blk){
        for (Buffer buff : bufferpool) {
            Block b = buff.block();
            if (b != null && b.equals(blk))
                return buff;
        }
        return null;
    }
}
