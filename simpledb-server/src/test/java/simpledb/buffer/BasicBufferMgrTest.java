package simpledb.buffer;

import org.junit.Before;
import org.junit.Test;
import simpledb.file.Block;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class BasicBufferMgrTest {

    private BasicBufferMgr basicBufferMgr;
    private int numBuffs;
    private List<Block> mockBlocks;
    private List<Buffer> mockBuffers;

    @Before
    public void setUp(){
        numBuffs  = 4;
        basicBufferMgr = new BasicBufferMgr(numBuffs);
        mockBlocks = new ArrayList<Block>();
        for(int i = 0; i < numBuffs+1; i++){
//            mockBlocks.add(mock(Block.class));
            mockBlocks.add(new Block("file" + i, i));
        }
        GClockBufferPool gClockBufferPool = basicBufferMgr.getBufferPool();
        mockBuffers = new ArrayList<Buffer>();
        for(Buffer buffer :  gClockBufferPool.getBufferpool()){
            Buffer buff = spy(buffer);
            doNothing().when(buff).assignToBlock(any(Block.class));
            mockBuffers.add(buff);
        }
        gClockBufferPool.setBufferPool( mockBuffers.toArray(new Buffer[numBuffs]));
    }

    @Test
    public void mapIsInitiallyEmpty(){
        for(Block block :  mockBlocks)
            assertFalse(basicBufferMgr.containsMapping(block));
    }

    @Test
    public void mappingIsMadeAfterFirstPin(){
        basicBufferMgr.pin(mockBlocks.get(0));
        assertTrue(basicBufferMgr.containsMapping(mockBlocks.get(0)));
    }

    @Test
    public void blocksAreAssignedInRoundRobin(){
        Buffer buff1 = basicBufferMgr.pin(mockBlocks.get(0));
        basicBufferMgr.unpin(buff1);
        Buffer buff2 = basicBufferMgr.pin(mockBlocks.get(1));
        assertTrue(!buff1.equals(buff2));
    }

    @Test
    public void shouldReturnNullIfAllBuffersArePinned(){
        for(int i = 0; i < numBuffs; i++)
            basicBufferMgr.pin(mockBlocks.get(i));
        // next pin request should yield null
        Buffer nullBuff = basicBufferMgr.pin(mockBlocks.get(numBuffs));
        assertTrue(nullBuff == null);
    }

    @Test
    public void testAvailabilityOfBuffer(){
        Buffer buff = basicBufferMgr.pin(mockBlocks.get(0));
        Buffer buff1 = basicBufferMgr.pin(mockBlocks.get(0));
        assertTrue(buff.equals(buff1));
        buff1.unpin();
        assertTrue(buff.isPinned());
        buff.unpin();
        assertFalse(buff.isPinned());
    }

    @Test
    public void bufferIsReassignedAfterAllBuffersAreUtilized(){
        Buffer buff0 = basicBufferMgr.pin(mockBlocks.get(0));
        buff0.unpin();
        for(int i = 1; i < numBuffs; i++) {
            Buffer buff = basicBufferMgr.pin(mockBlocks.get(i));
            buff.unpin();
        }
        Buffer reassign = basicBufferMgr.pin(mockBlocks.get(numBuffs));
        assertTrue(reassign.equals(buff0));
    }

    @Test
    public void newMappingsAreCreated(){
        Buffer buff0 = basicBufferMgr.pin(mockBlocks.get(0));
        buff0.unpin();
        for(int i = 1; i < numBuffs; i++) {
            Buffer buff = basicBufferMgr.pin(mockBlocks.get(i));
            buff.unpin();
        }
        basicBufferMgr.pin(mockBlocks.get(numBuffs));
        assertTrue(basicBufferMgr.containsMapping(mockBlocks.get(numBuffs)));

    }

    @Test
    public void oldMappingsAreRemoved(){
        Buffer buff0 = basicBufferMgr.pin(mockBlocks.get(0));
        buff0.unpin();
        assertTrue(basicBufferMgr.getMapping(mockBlocks.get(0)).equals(buff0));
        for(int i = 1; i < numBuffs; i++) {
            Buffer buff = basicBufferMgr.pin(mockBlocks.get(i));
            buff.unpin();
        }
        Buffer reassign = basicBufferMgr.pin(mockBlocks.get(numBuffs));

        assertTrue(basicBufferMgr.getMapping(mockBlocks.get(numBuffs)).equals(reassign));
        assertTrue(basicBufferMgr.containsMapping(mockBlocks.get(numBuffs)));

    }


}
